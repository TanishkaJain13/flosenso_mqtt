"""
server_logger.py
----------------
Device message history (registered users / MACs) plus MQTT viewer using
``config.json``. When the ``users`` table has rows, the main viewer uses
**username** and **MAC** from ``users``; the MQTT **customer_id** (third
``flosenso&<mac>&<customer>`` segment) is resolved from ``customers`` or recent
``mqtt_messages``. With no registered users, **customer_id** / **MAC** options
still come from ``mqtt_messages`` as before. The dual-broker ``mqtt_service``
ingests ``flosenso&<mac>&<customer_id>`` topics even when that MAC is not yet
in ``users`` (other topics still require allow-list registration).

Run:
    streamlit run main.py
"""

from __future__ import annotations

import datetime as _dt
import json
import sqlite3
import ssl
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import paho.mqtt.client as mqtt
import streamlit as st

from database import (
    DB_PATH,
    get_all_usernames,
    get_all_users,
    get_mac_ids_for_user,
    get_message_count_by_broker,
    get_messages_for_device,
    get_username_for_mac,
    init_db,
    list_customer_ids_from_flosenso_topics,
    list_distinct_mac_ids_from_mqtt_messages,
    list_mac_ids_for_flosenso_customer,
    resolve_flosenso_customer_id_for_mac,
    get_registered_mac_ids,
    verify_admin_login,
)
from mqtt_service import BROKERS
from utils import setup_logging

_IST = _dt.timezone(_dt.timedelta(hours=5, minutes=30))


def to_ist(df: pd.DataFrame, *cols: str) -> pd.DataFrame:
    """Convert timestamp columns from UTC to IST."""
    for col in cols:
        if col not in df.columns:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
        df[col] = parsed.dt.tz_convert(_IST).dt.strftime("%Y-%m-%d %H:%M:%S")
    return df


_APP_DIR = Path(__file__).resolve().parent
_REPORT_CONFIG_PATH = _APP_DIR / "config.json"


@st.cache_resource
def load_report_config():
    """
    Load config.json from the same directory as this module (not process cwd).
    Relative `database.path` entries are resolved against that directory.
    """
    try:
        with open(_REPORT_CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    db_rel = (cfg.get("database") or {}).get("path")
    if db_rel:
        p = Path(db_rel)
        if not p.is_absolute():
            cfg.setdefault("database", {})["path"] = str((_APP_DIR / p).resolve())
    return cfg


@st.cache_data
def cached_flosenso_customer_ids(db_path: str) -> tuple[str, ...]:
    return tuple(list_customer_ids_from_flosenso_topics(db_path))


@st.cache_data
def cached_flosenso_macs_for_customer(db_path: str, customer_id: str) -> tuple[str, ...]:
    return tuple(list_mac_ids_for_flosenso_customer(customer_id, db_path))


@st.cache_data(ttl=120, show_spinner=False)
def cached_distinct_mqtt_macs(db_path: str) -> tuple[str, ...]:
    return tuple(list_distinct_mac_ids_from_mqtt_messages(db_path))


@st.cache_data(ttl=60, show_spinner=False)
def cached_resolve_flosenso_customer_for_mac(db_path: str, mac_id: str) -> str | None:
    return resolve_flosenso_customer_id_for_mac(mac_id, db_path)


@st.cache_data(ttl=120, show_spinner=False)
def cached_mqtt_customer_username_labels(
    customer_ids: tuple[str, ...], db_path: str
) -> dict[str, str]:
    """
    Lazy-import ``mqtt_customer_username_labels`` so this page still loads if an
    older ``database.py`` (without that helper) is on ``sys.path`` by mistake.
    """
    try:
        from database import mqtt_customer_username_labels as _labels_fn
    except ImportError:
        return {str(c).strip(): str(c).strip() for c in customer_ids if str(c).strip()}
    return _labels_fn(list(customer_ids), db_path)


def get_report_messages_data(
    db_path: str, customer_id: str, mac_id: str, start_date=None, end_date=None
):
    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    topic_pattern = f"flosenso&{mac_id}&{customer_id}"
    sql = """
        SELECT broker_name, topic,mac_id, payload, timestamp
        FROM mqtt_messages
        WHERE topic = ?
    """
    params: list = [topic_pattern]
    if start_date is not None and end_date is not None:
        sql += " AND DATE(received_at) BETWEEN DATE(?) AND DATE(?)"
        params.extend([str(start_date), str(end_date)])
    sql += " ORDER BY received_at DESC"
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df


def publish_message_via_config(cfg: dict, topic: str, message: str) -> bool:
    """Publish using broker settings from config.json (report tab)."""
    try:
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2, clean_session=True
        )
        client.username_pw_set(cfg["broker"]["username"], cfg["broker"]["password"])
        if cfg["broker"]["port"] == 8883:
            client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS)
        client.connect(cfg["broker"]["address"], cfg["broker"]["port"], keepalive=60)
        client.loop_start()
        result = client.publish(topic, message, qos=1)
        client.loop_stop()
        client.disconnect()
        return result.rc == 0
    except Exception as e:
        st.error(f"Error: {e}")
        return False


def publish_message(
    topic: str,
    message: str,
    broker_name: str | None = None,
    qos: int = 1,
    retain: bool = False,
) -> tuple[bool, str]:
    """Publish using BROKERS from mqtt_service (device history Quick Actions)."""
    targets = (
        [b for b in BROKERS if b.name == broker_name] if broker_name else BROKERS
    )
    for cfg in targets:
        try:
            client = mqtt.Client(
                client_id="flosenso-dashboard-pub",
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                clean_session=True,
            )
            if cfg.username:
                client.username_pw_set(cfg.username, cfg.password)
            if cfg.use_tls:
                client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS)
            client.connect(cfg.host, cfg.port, keepalive=30)
            client.loop_start()
            result = client.publish(topic, message, qos=qos, retain=retain)
            result.wait_for_publish(timeout=5)
            client.loop_stop()
            client.disconnect()
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                return True, cfg.name
        except Exception as exc:
            st.warning(f"[{cfg.name}] publish failed: {exc}")
    return False, ""


def main() -> None:
    setup_logging()

    st.set_page_config(
        page_title="Flosenso MQTT Dashboard",
        page_icon="📡",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        # Custom CSS for login page
        st.markdown(
            """
            <style>
                .stApp {
                    background: #0e1117;
                }
                .login-container {
                    max-width: 400px;
                    margin: 100px auto;
                    padding: 2rem;
                    background: #1e1e2e;
                    border-radius: 12px;
                    border: 1px solid #313244;
                    box-shadow: 0 4px 24px rgba(0,0,0,0.4);
                }
                .stTextInput > div > div > input {
                    background-color: #313244 !important;
                    color: #cdd6f4 !important;
                }
            </style>
            """,
            unsafe_allow_html=True
        )
        
        st.markdown("<div style='height: 10vh'></div>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.markdown(
                """
                <div style='text-align: center; margin-bottom: 2rem;'>
                    <h1 style='color: #cdd6f4; font-size: 2.5rem; margin-bottom: 0.5rem;'>📡 Flosenso</h1>
                    <p style='color: #a6adc8; font-size: 1.1rem;'>Server Management Dashboard</p>
                </div>
                """, 
                unsafe_allow_html=True
            )
            
            with st.form("login_form"):
                st.subheader("🔐 Admin Login")
                username = st.text_input("Username", placeholder="Enter username")
                password = st.text_input("Password", type="password", placeholder="Enter password")
                submit = st.form_submit_button("Sign In", use_container_width=True, type="primary")
                
                if submit:
                    cfg = load_report_config()
                    db_path = cfg["database"]["path"] if cfg else DB_PATH
                    # Ensure DB is initialized to create default admin if needed
                    init_db(db_path)
                    
                    if verify_admin_login(username, password, db_path):
                        st.session_state.authenticated = True
                        st.session_state.username = username
                        st.success("Welcome back!")
                        st.rerun()
                    else:
                        st.error("Invalid credentials")
        return


    st.markdown(
        """
        <style>
            [data-testid="metric-container"] {
                background: #1e1e2e;
                border: 1px solid #313244;
                border-radius: 8px;
                padding: 12px 16px;
            }
            button[data-baseweb="tab"] { font-size: 15px; font-weight: 600; }
            thead tr th { background-color: #1e1e2e !important; color: #cdd6f4 !important; }
            .stAlert { border-radius: 8px; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Header with Logout Button
    header_col, logout_col = st.columns([5, 1])
    with header_col:
        st.title("📡 Flosenso MQTT Dashboard")
    with logout_col:
        st.write("") # Vertical spacing
        st.write("")
        if st.button("🚪 Logout", key="logout_top", use_container_width=True,type="primary"):
            st.session_state.authenticated = False
            st.rerun()
    st.divider()
    broker_counts = get_message_count_by_broker()
    cols_stats = st.columns(max(len(broker_counts), 1))
    for i, bc in enumerate(broker_counts):
        with cols_stats[i]:
            st.metric(f"📨 {bc['broker_name']}", f"{bc['total']:,} msgs")

    # st.header("MQTT Messages Viewer & Control")
    cfg = load_report_config()
    if not cfg:
        st.error(
            f"Config file not found or invalid. Expected: `{_REPORT_CONFIG_PATH}` "
            "(loaded next to `server_logger.py`, not from the shell working directory)."
        )
    else:
        
        db_path = cfg["database"]["path"]
        init_db(db_path)
        if "selected_customer" not in st.session_state:
            st.session_state.selected_customer = None
        if "selected_mac" not in st.session_state:
            st.session_state.selected_mac = None
        # col1, col2 = st.columns(2)
        # usernames_tab1 = get_all_usernames(db_path)
        # selected_customer: str | None = None
        # selected_mac_r: str | None = None

        # if usernames_tab1:
        #     with col1:
        #         selected_user_tab1 = st.selectbox(
        #             "Customer ID",
        #             options=usernames_tab1,
        #             key="view_user_tab1",
        #         )
        #     mac_opts_tab1 = get_mac_ids_for_user(selected_user_tab1, db_path)
        #     with col2:
        #         if mac_opts_tab1:
        #             default_idx = 0
        #             if st.session_state.selected_mac and st.session_state.selected_mac in mac_opts_tab1:
        #                 default_idx = mac_opts_tab1.index(st.session_state.selected_mac)
        #             selected_mac_r = st.selectbox(
        #                 "Select MAC ID",
        #                 mac_opts_tab1,
        #                 index=default_idx,
        #                 key="view_mac",
        #             )
        #             st.session_state.selected_mac = selected_mac_r
        #             resolved_c = cached_resolve_flosenso_customer_for_mac(
        #                 db_path, selected_mac_r
        #             )
        #             if resolved_c:
        #                 selected_customer = resolved_c
        #                 st.session_state.selected_customer = resolved_c
        #                 st.caption(f"MQTT customer segment (resolved): **{resolved_c}**")
        #             else:
        #                 st.session_state.selected_customer = None
        #                 st.warning(
        #                     "No **customer_id** found for this MAC — register traffic on "
        #                     "`flosenso&<mac>&<customer>` or wait for **customers** sync."
        #                 )
        #         else:
        #             st.info("No MAC IDs registered for this user in **users**.")
        #             st.session_state.selected_mac = None
        #             st.session_state.selected_customer = None
        # else:
        #     with col1:
        #         customer_ids = list(cached_flosenso_customer_ids(db_path))
        #         if customer_ids:
        #             default_idx = 0
        #             if (
        #                 st.session_state.selected_customer
        #                 and st.session_state.selected_customer in customer_ids
        #             ):
        #                 default_idx = customer_ids.index(st.session_state.selected_customer)
        #             selected_customer = st.selectbox(
        #                 "Select Customer ID",
        #                 customer_ids,
        #                 index=default_idx,
        #                 key="view_customer",
        #             )
        #             st.session_state.selected_customer = selected_customer
        #         else:
        #             st.info(
        #                 "No **customer_id** values yet. Publish on "
        #                 "`flosenso&<mac_id>&<customer_id>` (the MQTT service stores these "
        #                 "without pre-registering the MAC), or add rows to **users** to pick "
        #                 "username / MAC from the allow-list."
        #             )
        #             selected_customer = None
        #     with col2:
        #         if selected_customer:
        #             mac_ids_r = list(
        #                 cached_flosenso_macs_for_customer(db_path, selected_customer)
        #             )
        #             if mac_ids_r:
        #                 default_idx = 0
        #                 if st.session_state.selected_mac and st.session_state.selected_mac in mac_ids_r:
        #                     default_idx = mac_ids_r.index(st.session_state.selected_mac)
        #                 selected_mac_r = st.selectbox(
        #                     "Select MAC ID",
        #                     mac_ids_r,
        #                     index=default_idx,
        #                     key="view_mac",
        #                 )
        #                 st.session_state.selected_mac = selected_mac_r
        #             else:
        #                 st.info(
        #                     "No **mac_id** values for this customer — check `mqtt_messages` "
        #                     "rows where `topic` matches `flosenso&…&` + customer."
        #                 )
        #                 selected_mac_r = None
        #         else:
        #             selected_mac_r = None
        # if selected_customer and selected_mac_r:
        #     st.divider()
        #     load_data = st.button("🔍 Load Data", key="load_btn", use_container_width=True)
        #     if load_data:
        #         cached_flosenso_customer_ids.clear()
        #         cached_flosenso_macs_for_customer.clear()
        #         cached_resolve_flosenso_customer_for_mac.clear()
        #         df_r = get_report_messages_data(
        #             db_path, selected_customer, selected_mac_r, None, None
        #         )
        #         if not df_r.empty:
        #             st.success(f"Found {len(df_r)} messages")
        #             df_r["timestamp"] = pd.to_datetime(df_r["timestamp"], errors="coerce")
        #             df_r["Time stamp"] = df_r["timestamp"].dt.strftime("%d-%b-%Y %I:%M %p")
        #             df_r = df_r.drop(columns=["timestamp", "received_at"])
        #             st.session_state.df_loaded = df_r
        #         else:
        #             st.info("No messages found for this combination")
        #             st.session_state.df_loaded = None
        #     if "df_loaded" in st.session_state and st.session_state.df_loaded is not None:
        #         st.dataframe(st.session_state.df_loaded, use_container_width=True, height=400)
        #         csv_r = st.session_state.df_loaded.to_csv(index=False)
        #         st.download_button(
        #             label="📥 Download CSV",
        #             data=csv_r,
        #             file_name=(
        #                 f"mqtt_data_{selected_mac_r}_{selected_customer}_all_time.csv"
        #             ),
        #             mime="text/csv",
        #         )
        #     st.divider()
            # topic_r = f"flosenso&{selected_mac_r}&{selected_customer}"
            
            # st.info(f"📡 Publishing to: **{topic_r}**")
            # st.subheader("Quick Actions")
            # commands = [
            #     ("🔵 GET_STATUS", "app200req"),
            #     ("📶 GET_WIFI_STRENGTH", "app308"),
            #     ("📏 CHECK_DISTANCE", "app301&T"),
            #     ("⚙️ GET_SETTINGS", "app300"),
            #     ("📅 GET_SCHEDULES", "app204getsdl"),
            #     ("🔁 RESTART_DEVICE", "app302"),
            #     ("🔄 RESET_LORA", "app304"),
            #     ("🔌 FORCE_MOTOR_ON", "app210&MO&05"),
            #     ("⛔ FORCE_MOTOR_OFF", "app210&MF"),
            # ]
            # cols_btn = st.columns(5)
            # i = 0
            # for label, cmd in commands:
            #     with cols_btn[i % 5]:
            #         if st.button(label, key=f"btn_{i}", use_container_width=True):
            #             if publish_message_via_config(cfg, topic_r, cmd):
            #                 st.success("✅ Sent!")
            #             else:
            #                 st.error("❌ Failed")
            #     i += 1
            #     if i % 5 == 0:
            #         cols_btn = st.columns(5)
            # st.write("")
            # col_a, col_b = st.columns([3, 1])
            # with col_a:
            #     custom_message = st.text_input("Enter custom message", key="custom_msg_tab1")
            # with col_b:
            #     st.write("")
            #     st.write("")
            #     if st.button("📤 Send Custom", key="send_custom_tab1", use_container_width=True):
            #         if custom_message:
            #             if publish_message_via_config(cfg, topic_r, custom_message):
            #                 st.success("✅ Sent!")
            #             else:
            #                 st.error("❌ Failed")
            #         else:
            #             st.warning("Enter message")
    st.divider()

    st.subheader("Device Message History")
    db_for_viewer = cfg["database"]["path"] if cfg else DB_PATH
    usernames_reg = get_all_usernames(db_for_viewer)
    mqtt_customers = (
        list(cached_flosenso_customer_ids(db_for_viewer)) if not usernames_reg else []
    )
    mqtt_mac_ids = (
        list(cached_distinct_mqtt_macs(db_for_viewer))
        if not usernames_reg and not mqtt_customers
        else []
    )

    if usernames_reg:
        viewer_source = "registered"
    elif mqtt_customers:
        viewer_source = "mqtt_customer"
    elif mqtt_mac_ids:
        viewer_source = "mqtt_mac"
    else:
        viewer_source = None

    if viewer_source is None:
        st.warning(
            "No devices on the allow-list and no usable rows in **mqtt_messages**. "
            "Ensure the **mqtt_service** (or **app.py** logger) is running against this DB, "
            "then publish on `flosenso&<mac>&<customer>` **or** register MACs under **users** "
            "so other topics are ingested."
        )
    else:
        if "device_hist_df" not in st.session_state:
            st.session_state.device_hist_df = None
            st.session_state.device_hist_loaded_at = None
            st.session_state.device_hist_empty_mac = None
            st.session_state.device_hist_metrics = None

        sel_col1, sel_col2, sel_col3, sel_col4, sel_col5 = st.columns([1, 1, 1, 1, 1])
        selected_mac: str | None = None

        if viewer_source == "registered":
            all_reg_macs = sorted(list(get_registered_mac_ids(db_for_viewer)))
            
            def on_viewer_mac_change():
                new_mac = st.session_state.viewer_mac
                if new_mac and new_mac != "Select the ID":
                    u = get_username_for_mac(new_mac, db_for_viewer)
                    if u:
                        st.session_state.viewer_user = u

            with sel_col1:
                selected_user = st.selectbox(
                    "Customer ID",
                    options=["Select the ID"] + list(usernames_reg),
                    key="viewer_user",
                )
            
            mac_options = ["Select the ID"]
            if selected_user != "Select the ID":
                mac_options += list(get_mac_ids_for_user(selected_user, db_for_viewer))
            else:
                mac_options += all_reg_macs

            with sel_col2:
                selected_mac = st.selectbox(
                    "MAC ID",
                    options=mac_options,
                    key="viewer_mac",
                    on_change=on_viewer_mac_change
                )
        elif viewer_source == "mqtt_customer":
            _cust_key = tuple(sorted(mqtt_customers))
            _mqtt_user_labels = cached_mqtt_customer_username_labels(
                _cust_key, db_for_viewer
            )
            
            def on_viewer_mac_mqtt_change():
                new_mac = st.session_state.viewer_mac_mqtt
                if new_mac and new_mac != "Select the MAC ID":
                    c = resolve_flosenso_customer_id_for_mac(new_mac, db_for_viewer)
                    if c:
                        st.session_state.viewer_mqtt_customer = c

            with sel_col1:
                selected_username_mqtt = st.selectbox(
                    "Customer ID",
                    options=["Select the Customer ID"] + list(mqtt_customers),
                    key="viewer_mqtt_customer",
                    format_func=lambda c, lb=_mqtt_user_labels: lb.get(c, c),
                )
            
            mac_options = ["Select the MAC ID"]
            if selected_username_mqtt != "Select the Customer ID":
                mac_options += list(cached_flosenso_macs_for_customer(db_for_viewer, selected_username_mqtt))
            else:
                # Show all MACs that have customer associations
                all_mqtt_macs = sorted(list(cached_distinct_mqtt_macs(db_for_viewer)))
                mac_options += all_mqtt_macs

            with sel_col2:
                selected_mac = st.selectbox(
                    "MAC ID",
                    options=mac_options,
                    key="viewer_mac_mqtt",
                    on_change=on_viewer_mac_mqtt_change
                )
        else:
            with sel_col1:
                selected_mac = st.selectbox(
                    "MAC ID (from MQTT)",
                    options=["Select the MAC ID"] + list(mqtt_mac_ids),
                    key="viewer_mac_only",
                )
        
        if selected_mac in ["Select the MAC ID", "Select the ID"]:
            selected_mac = None

        with sel_col3:
            hist_start = st.date_input(
                "Start Date",
                datetime.now() - timedelta(days=30),
                key="viewer_hist_start",
            )
        with sel_col4:
            hist_end = st.date_input("End Date", datetime.now(), key="viewer_hist_end")
        
        broker_names = [b.name for b in BROKERS]
        qa_b_row1, qa_b_row2 = st.columns([0.5, 0.5])
        with qa_b_row1:
            selected_broker = st.selectbox(
                "Send via broker",
                options=["Auto (try all)"] + broker_names,
                key="qa_broker_hist",
                help="Restrict loaded history to this broker and publish Quick Actions through it.",
            )
        with qa_b_row2:
            st.markdown("<div style='margin-top:27px'>", unsafe_allow_html=True)
            device_hist_load = st.button(
                "🔄  Load Data",
                use_container_width=True,
                type="primary",
                key="viewer_hist_load",
            )
            st.markdown("</div>", unsafe_allow_html=True)

        broker_arg = None if selected_broker == "Auto (try all)" else selected_broker

        if device_hist_load:
            cached_flosenso_customer_ids.clear()
            cached_flosenso_macs_for_customer.clear()
            cached_distinct_mqtt_macs.clear()
            cached_mqtt_customer_username_labels.clear()
            if not selected_mac:
                st.warning(
                    "Select a MAC ID (or pick a user that has one), then click **Load Data**."
                )
            elif hist_start > hist_end:
                st.warning("**Start Date** must be on or before **End Date**.")
            else:
                messages = get_messages_for_device(
                    selected_mac,
                    db_path=db_for_viewer,
                    start_date=hist_start,
                    end_date=hist_end,
                    broker_name=broker_arg,
                )
                loaded_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if not messages:
                    st.session_state.device_hist_df = pd.DataFrame()
                    st.session_state.device_hist_empty_mac = selected_mac
                    st.session_state.device_hist_metrics = None
                    st.session_state.device_hist_loaded_at = loaded_at
                    st.session_state.device_hist_publish_topic = f"flosenso&{selected_mac}"
                    st.session_state.device_hist_qa_mac = selected_mac
                else:
                    df_dev = pd.DataFrame(messages)
                    df_dev = df_dev.rename(
                        columns={
                            "broker_name": "Broker",
                            "topic": "Topic",
                            "mac_id": "MAC ID",
                            "payload": "Payload",
                            "qos": "QoS",
                            "retain": "Retain",
                            "timestamp": "Timestamp (IST)",
                            "received_at": "Received At (IST)",
                        }
                    )
                    df_dev = to_ist(df_dev, "Received At (IST)")
                    total_dev = len(df_dev)
                    df_dev_show = df_dev[
                        [
                            "Broker",
                            "Topic",
                            "MAC ID",
                            "Payload",
                            "Timestamp (IST)",
                         
                        ]
                    ].copy()
                    df_dev_show.insert(0, "S.No", range(1, total_dev + 1))
                    st.session_state.device_hist_df = df_dev_show
                    st.session_state.device_hist_empty_mac = None
                    st.session_state.device_hist_metrics = {
                        "total": total_dev,
                        "brokers": int(df_dev["Broker"].nunique()),
                        "topics": int(df_dev["Topic"].nunique()),
                        "date_range": f"{hist_start} → {hist_end}",
                        "mac": selected_mac,
                    }
                    st.session_state.device_hist_loaded_at = loaded_at
                    publish_topic = f"flosenso&{selected_mac}"
                    raw_topic = messages[0].get("topic", "")
                    if len(raw_topic.split("&")) == 3:
                        publish_topic = raw_topic
                    st.session_state.device_hist_publish_topic = publish_topic
                    st.session_state.device_hist_qa_mac = selected_mac

        df_hist = st.session_state.device_hist_df
        loaded_hist_at = st.session_state.device_hist_loaded_at

        if df_hist is None:
            st.info(
                "👆 Choose User ID / MAC ID (when shown), **Start Date** / **End Date**, "
                "then click **Load Data**."
            )
        elif df_hist.empty:
            em = st.session_state.get("device_hist_empty_mac") or "—"
            st.info(f"No messages found for **{em}** in the selected time range.")
            if loaded_hist_at:
                st.caption(f"Loaded at **{loaded_hist_at}**")
        else:
            met = st.session_state.device_hist_metrics or {}
            total_dev = int(met.get("total", len(df_hist)))
            m1, m2, m3 = st.columns(3)
            m1.metric("Total Records", f"{total_dev:,}")
            m2.metric("Brokers Seen", met.get("brokers", "—"))
            m3.metric("Unique Topics", met.get("topics", "—"))
          
            mac_caption = met.get("mac", "")
            st.caption(
                f"Showing **{total_dev:,}** record(s) for `{mac_caption}` · "
                f"loaded **{loaded_hist_at or '—'}**"
            )
            st.dataframe(
                df_hist,
                use_container_width=True,
                hide_index=True,
                height=380,
            )
            dr = str(met.get("date_range", "history")).replace(" ", "").replace("→", "_to_")
            csv = df_hist.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Download CSV",
                data=csv,
                file_name=f"{str(mac_caption).replace(':', '')}_{dr}.csv",
                mime="text/csv",
            )

        if selected_mac:
            st.divider()
            st.subheader("⚡ Quick Actions")
            publish_topic = f"flosenso&{selected_mac}"
            if (
                st.session_state.get("device_hist_qa_mac") == selected_mac
                and st.session_state.get("device_hist_publish_topic")
            ):
                publish_topic = st.session_state["device_hist_publish_topic"]

            st.markdown(
                f"<div style='margin-top:8px'>📡 Publishing to: <b>{publish_topic}</b> · "
                f"Broker: <b>{selected_broker}</b></div>",
                unsafe_allow_html=True,
            )

            COMMANDS = [
                ("🔵 GET_STATUS", "app200req"),
                ("📶 GET_WIFI_STRENGTH", "app308"),
                ("📏 CHECK_DISTANCE", "app301&T"),
                ("⚙️ GET_SETTINGS", "app300"),
                ("📅 GET_SCHEDULES", "app204getsdl"),
                ("🔁 RESTART_DEVICE", "app302"),
                ("🔄 RESET_LORA", "app304"),
                ("🔌 FORCE_MOTOR_ON", "app210&MO&05"),
                ("⛔ FORCE_MOTOR_OFF", "app210&MF"),
            ]

            # Filter commands for specific user
            if st.session_state.get("username") == "flosenso.cc@hipl.co.in":
                COMMANDS = [cmd for cmd in COMMANDS if cmd[0] not in ["📏 CHECK_DISTANCE", "🔄 RESET_LORA"]]
            NUM_COLS = 5
            for row_start in range(0, len(COMMANDS), NUM_COLS):
                row_cmds = COMMANDS[row_start : row_start + NUM_COLS]
                btn_cols = st.columns(NUM_COLS)
                for col_idx, (label, cmd) in enumerate(row_cmds):
                    with btn_cols[col_idx]:
                        if st.button(
                            label,
                            key=f"qa_btn_hist_{row_start + col_idx}",
                            use_container_width=True,
                        ):
                            ok, used = publish_message(
                                publish_topic, cmd, broker_name=broker_arg
                            )
                            if ok:
                                st.success(f"✅ `{cmd}` sent via **{used}**")
                            else:
                                st.error(f"❌ Failed to send `{cmd}`")

            st.write("")
            cust_col1, cust_col2 = st.columns([4, 1])
            with cust_col1:
                custom_msg = st.text_input(
                    "Enter custom message",
                    placeholder="e.g.  app210&MO&10",
                    key="viewer_custom_msg_hist",
                )
            with cust_col2:
                st.markdown("<div style='margin-top:27px'>", unsafe_allow_html=True)
                if st.button(
                    "📤 Send Custom",
                    key="viewer_send_custom_hist",
                    use_container_width=True,
                ):
                    if custom_msg.strip():
                        ok, used = publish_message(
                            publish_topic,
                            custom_msg.strip(),
                            broker_name=broker_arg,
                        )
                        if ok:
                            st.success(f"✅ `{custom_msg.strip()}` sent via **{used}**")
                        else:
                            st.error("❌ Failed to send message")
                    else:
                        st.warning("Please enter a message before sending.")
                st.markdown("</div>", unsafe_allow_html=True)


if __name__ == "__main__":
    main()
