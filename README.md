# Flosenso MQTT Dashboard

Ingests MQTT messages from two HiveMQ brokers into SQLite and serves a device
message-history dashboard with live device control (publish).

The system has **three** parts that run independently:

| Part | Path | Role |
| --- | --- | --- |
| Ingestion service | `mqtt_service.py` | Subscribes to both brokers, writes to `mqtt_data.db`. |
| API backend | `backend/` (FastAPI) | Reads the DB + publishes commands, for the React UI. |
| Frontend | `frontend/` (React + Vite) | The dashboard UI. |

Shared modules: `database.py` (SQLite data layer) and `utils.py` (topic/MAC/time
helpers) are used by both the ingestion service and the backend.

---

## 1. Ingestion service (keep running)

```bash
source venv/bin/activate
python mqtt_service.py
```

This is unchanged — it connects to the brokers in `mqtt_service.BROKERS` and
batch-writes messages into `mqtt_data.db`.

## 2. FastAPI backend

```bash
source venv/bin/activate
pip install -r backend/requirements.txt
uvicorn backend.main:app --reload --port 8000
```

Environment variables (all optional):

- `FLOSENSO_DB_PATH` — path to the SQLite DB (default `./mqtt_data.db`).
- `FLOSENSO_CORS_ORIGINS` — comma-separated allowed origins
  (default `http://localhost:5173,http://127.0.0.1:5173`).

> **Auth:** login is **static and handled entirely in the frontend** (see
> `frontend/src/auth.js`). The API itself is open (no token). This gates the UI
> only — it is not a security boundary, since the credentials ship in the bundle.
> Put the API behind a network boundary / reverse proxy if it must not be public.

### Key endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/stats/broker-counts` | Per-broker message totals. |
| GET | `/api/devices` | Customer IDs, MAC IDs and pairings (from the `customers` table). |
| GET | `/api/messages` | Message history (`mac_id`, `customer_id`, `start_date`, `end_date`, `broker`, `payload`). |
| GET | `/api/commands` | Quick Action command list (frontend hides some for the `flosenso.cc` user). |
| POST | `/api/publish` | Publish a command to a device topic. |

## 3. Frontend

```bash
cd frontend
npm install
npm run dev          # http://localhost:5173 (proxies /api → :8000)
```

Build for production:

```bash
npm run build        # outputs to frontend/dist
```

Static logins (defined in `frontend/src/auth.js`):
`admin` / `admin123`, and `flosenso.cc@hipl.co.in` / `flosenso@cc123`
(the latter has a couple of Quick Action commands hidden).

---

## Performance changes

This rebuild targeted dashboard load time:

1. **Indexing `mqtt_messages`** — added composite indexes that are applied to the
   existing `mqtt_data.db` the next time `init_db` runs (on backend startup):
   - `idx_messages_topic_received (topic, received_at DESC)` — the report query
     (`WHERE topic = ?`) was a full table SCAN; it now uses this index.
   - `idx_messages_mac_timestamp (mac_id, timestamp DESC)` — device history by MAC.
   - `idx_messages_broker_ts (broker_name, timestamp DESC)` — per-broker scoping.

2. **Customer/MAC lists from the `customers` table** — the dropdowns previously
   scanned every row of `mqtt_messages`. `/api/devices` now reads the small,
   indexed `customers` table in one query (`list_customer_mac_pairs_from_customers`),
   and message queries use `query_messages_fast`, which builds exact predicates so
   the new indexes are actually used.
```
