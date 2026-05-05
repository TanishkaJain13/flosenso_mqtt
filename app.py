import os
import paho.mqtt.client as mqtt
import sqlite3
import logging
import time
from typing import Optional
import signal
import sys
from threading import Thread, current_thread, main_thread
from queue import Queue

from database import init_db
from utils import extract_mac_from_topic, normalise_mac, utc_now_str

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hivemq_service.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def _is_flosenso_routed_topic(topic: str) -> bool:
    parts = topic.split("&")
    if len(parts) != 3:
        return False
    if (parts[0] or "").strip().lower() != "flosenso":
        return False
    return bool((parts[1] or "").strip()) and bool((parts[2] or "").strip())


class HiveMQService:
    
    def __init__(self, broker_url: str, port: int, username: str, password: str,
                 broker_name: str,
                 topic: str = "#", db_path: str = "mqtt_data.db", batch_size: int = 200):
        self.broker_url = broker_url.strip()
        self.port = port
        self.username = username
        self.password = password
        self.topic = topic
        self.db_path = db_path
        self.batch_size = batch_size
        self.broker_name = broker_name.strip()
        self.client: Optional[mqtt.Client] = None
        self.running = False
        self.message_queue = Queue(maxsize=10000)
        self.db_thread = None
        self.total_messages = 0
        self.last_stats_time = time.time()
        self.messages_since_last_stats = 0
        self._init_database()
        
    def _init_database(self):
        try:
            init_db(self.db_path)
            logger.info("Database initialised (shared schema) at %s", self.db_path)
        except sqlite3.Error as e:
            logger.error(f"Database initialization error: {e}")
            raise
    
    def _database_worker(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA synchronous=NORMAL')
        cursor = conn.cursor()
        batch = []
        
        while self.running or not self.message_queue.empty():
            try:
                timeout = 0.02 if batch else 0.2
                try:
                    msg_data = self.message_queue.get(timeout=timeout)
                    batch.append(msg_data)
                except:
                    pass
                
                if len(batch) >= self.batch_size or (batch and time.time() - self.last_stats_time > 0.1):
                    cursor.executemany('''
                        INSERT INTO mqtt_messages
                            (topic, mac_id, payload, qos, retain, broker_name, timestamp, received_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', batch)
                    conn.commit()
                    self.total_messages += len(batch)
                    self.messages_since_last_stats += len(batch)
                    batch = []
                
                current_time = time.time()
                if current_time - self.last_stats_time >= 10:
                    rate = self.messages_since_last_stats / (current_time - self.last_stats_time)
                    logger.info(f"Rate: {rate:.2f} msg/sec | Total: {self.total_messages} messages")
                    self.last_stats_time = current_time
                    self.messages_since_last_stats = 0
            except Exception as e:
                logger.error(f"Database worker error: {e}")
                time.sleep(0.1)
        
        if batch:
            try:
                cursor.executemany('''
                    INSERT INTO mqtt_messages
                        (topic, mac_id, payload, qos, retain, broker_name, timestamp, received_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', batch)
                conn.commit()
                self.total_messages += len(batch)
            except Exception as e:
                logger.error(f"Error flushing final batch: {e}")
        
        conn.close()
        logger.info(f"Database worker stopped. Total messages stored: {self.total_messages}")
    
    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            logger.info("Connected successfully to broker (%s)", self.broker_name)
            try:
                client.subscribe(self.topic, qos=1)
                logger.info(f"Subscribed to topic: {self.topic}")
            except Exception as e:
                logger.error(f"Error subscribing to topic: {e}")
        else:
            error_messages = {
                1: "Connection refused - incorrect protocol version",
                2: "Connection refused - invalid client identifier",
                3: "Connection refused - server unavailable",
                4: "Connection refused - bad username or password",
                5: "Connection refused - not authorized"
            }
            logger.error(f"Connection failed: {error_messages.get(rc, f'Unknown error code: {rc}')}")
    
    def on_disconnect(self, client, userdata, rc, properties=None):
        if rc != 0:
            logger.warning(f"Unexpected disconnection (code: {rc}). Will attempt to reconnect...")
        else:
            logger.info("Disconnected from broker")
    
    def on_message(self, client, userdata, msg, properties=None):
        try:
            flos = _is_flosenso_routed_topic(msg.topic)
            mac_id = extract_mac_from_topic(msg.topic)
            if not mac_id and flos:
                mac_id = normalise_mac(msg.topic.split("&")[1])
            if not mac_id:
                return

            try:
                payload_str = msg.payload.decode('utf-8')
            except UnicodeDecodeError:
                payload_str = msg.payload.hex()

            ts = utc_now_str()
            msg_data = (
                msg.topic,
                mac_id,
                payload_str,
                msg.qos,
                int(msg.retain),
                self.broker_name,
                ts,
                ts,
            )
            self.message_queue.put(msg_data, block=False)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    def on_subscribe(self, client, userdata, mid, granted_qos, properties=None):
        logger.info(f"Subscription confirmed with QoS: {granted_qos}")
    
    def on_log(self, client, userdata, level, buf, properties=None):
        logger.debug(f"MQTT Log: {buf}")
    
    def start(self):
        try:
            self.running = True
            self.db_thread = Thread(target=self._database_worker, daemon=True)
            self.db_thread.start()
            logger.info("Database worker thread started")
            
            self.client = mqtt.Client(client_id=f"hivemq_service_{int(time.time())}",callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                                     clean_session=True)
            self.client.on_connect = self.on_connect
            self.client.on_disconnect = self.on_disconnect
            self.client.on_message = self.on_message
            self.client.on_subscribe = self.on_subscribe
            self.client.on_log = self.on_log
            self.client.username_pw_set(self.username, self.password)
            
            if self.port == 8883:
                import ssl
                self.client.tls_set(cert_reqs=ssl.CERT_REQUIRED, 
                                   tls_version=ssl.PROTOCOL_TLS)
                logger.info("TLS enabled for secure connection")
            
            self.client.reconnect_delay_set(min_delay=1, max_delay=120)
            logger.info(
                "Connecting to broker %s at %s:%s",
                self.broker_name,
                self.broker_url,
                self.port,
            )
            self.client.connect(self.broker_url, self.port, keepalive=60)
            logger.info("Starting MQTT loop...")
            self.client.loop_forever()
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received")
            self.stop()
        except Exception as e:
            logger.error(f"Error starting service: {e}")
            self.stop()
            raise
    
    def stop(self):
        logger.info("Stopping HiveMQ service...")
        self.running = False
        if self.client:
            try:
                self.client.disconnect()
                self.client.loop_stop()
                logger.info("MQTT client stopped successfully")
            except Exception as e:
                logger.error(f"Error stopping MQTT client: {e}")
        if self.db_thread and self.db_thread.is_alive():
            logger.info("Waiting for database worker to finish...")
            self.db_thread.join(timeout=5)
        logger.info("HiveMQ service stopped")


def signal_handler(signum, frame):
    logger.info(f"Received signal {signum}")
    sys.exit(0)


def start_service(broker):
    try:
        service = HiveMQService(
            broker_url=broker.host,
            port=broker.port,
            username=broker.username,
            password=broker.password,
            broker_name=broker.name,
            topic="#",
            db_path="mqtt_data.db",
            batch_size=5000
        )
        service.start()
    except Exception as e:
        logger.error(f"Fatal error for {broker.name}: {e}")

def main():
    from mqtt_service import BROKERS

    # Streamlit (and other hosts) run the script off the main thread; signal() requires main thread.
    if current_thread() is main_thread():
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    else:
        logger.info("Skipping POSIX signal handlers (not running on main thread, e.g. Streamlit)")

    logger.info("=" * 60)
    logger.info("HiveMQ Service Starting (Dual-Broker mode)")
    logger.info("=" * 60)
    
    threads = []
    for broker in BROKERS:
        t = Thread(target=start_service, args=(broker,), daemon=True)
        t.start()
        threads.append(t)
        
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
        sys.exit(0)


if __name__ == "__main__":
    main()