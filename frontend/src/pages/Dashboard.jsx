import React, { useEffect, useMemo, useState, useCallback } from "react";
import { api } from "../api.js";
import { CC_USER, CC_HIDDEN_COMMANDS } from "../auth.js";

const AUTO = "Auto (try all)";
const PICK_CUSTOMER = "Select the Customer ID";
const PICK_MAC = "Select the MAC ID";

function isoDate(d) {
  return d.toISOString().slice(0, 10);
}
function daysAgo(n) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d;
}

export default function Dashboard({ user, onLogout }) {
  // Reference data
  const [devices, setDevices] = useState({ customers: [], macs: [], macs_by_customer: {} });
  const [brokers, setBrokers] = useState([]);
  const [brokerCounts, setBrokerCounts] = useState([]);
  const [commands, setCommands] = useState([]);

  // Selectors
  const [customer, setCustomer] = useState(PICK_CUSTOMER);
  const [mac, setMac] = useState(PICK_MAC);
  const [startDate, setStartDate] = useState(isoDate(daysAgo(30)));
  const [endDate, setEndDate] = useState(isoDate(new Date()));
  const [loadBroker, setLoadBroker] = useState(AUTO);

  // Results
  const [rows, setRows] = useState(null); // null = nothing loaded yet
  const [metrics, setMetrics] = useState(null);
  const [loadedAt, setLoadedAt] = useState(null);
  const [loading, setLoading] = useState(false);
  const [notice, setNotice] = useState("");

  // Quick actions
  const [actionBroker, setActionBroker] = useState(AUTO);
  const [customMsg, setCustomMsg] = useState("");
  const [publishMsg, setPublishMsg] = useState("");

  const loadReference = useCallback(async () => {
    try {
      const [dev, br, bc, cmd] = await Promise.all([
        api.devices(),
        api.brokers(),
        api.brokerCounts(),
        api.commands(),
      ]);
      setDevices(dev);
      setBrokers(br.brokers || []);
      setBrokerCounts(bc.counts || []);
      let cmds = cmd.commands || [];
      // Restricted account: hide a couple of commands (was backend-side under JWT).
      if (user === CC_USER) {
        cmds = cmds.filter((c) => !CC_HIDDEN_COMMANDS.includes(c.label));
      }
      setCommands(cmds);
    } catch (err) {
      setNotice(err.message);
    }
  }, [user]);

  useEffect(() => {
    loadReference();
  }, [loadReference]);

  // MAC options depend on the chosen customer.
  const macOptions = useMemo(() => {
    if (customer && customer !== PICK_CUSTOMER) {
      return devices.macs_by_customer[customer] || [];
    }
    return devices.macs;
  }, [customer, devices]);

  // Selecting a MAC auto-fills its customer (mirrors the Streamlit behaviour).
  function onMacChange(value) {
    setMac(value);
    if (value && value !== PICK_MAC && (customer === PICK_CUSTOMER || !customer)) {
      const owner = Object.entries(devices.macs_by_customer).find(([, macs]) =>
        macs.includes(value)
      );
      if (owner) setCustomer(owner[0]);
    }
  }

  function onCustomerChange(value) {
    setCustomer(value);
    // Reset MAC if it no longer belongs to the selected customer.
    const allowed =
      value && value !== PICK_CUSTOMER ? devices.macs_by_customer[value] || [] : devices.macs;
    if (mac !== PICK_MAC && !allowed.includes(mac)) setMac(PICK_MAC);
  }

  const selectedMac = mac !== PICK_MAC ? mac : null;
  const selectedCustomer = customer !== PICK_CUSTOMER ? customer : null;

  // Topic Quick Actions publish to: exact topic from loaded rows, else flosenso&<mac>.
  const publishTopic = useMemo(() => {
    if (rows && rows.length) {
      const t = rows[0].topic || "";
      if (t.split("&").length === 3) return t;
    }
    if (selectedMac && selectedCustomer) return `flosenso&${selectedMac}&${selectedCustomer}`;
    if (selectedMac) return `flosenso&${selectedMac}`;
    return null;
  }, [rows, selectedMac, selectedCustomer]);

  async function loadData() {
    setNotice("");
    setPublishMsg("");
    if (!selectedMac && !selectedCustomer) {
      setNotice("Select a Customer ID and/or MAC ID, then click Load Data.");
      return;
    }
    if (startDate > endDate) {
      setNotice("Start Date must be on or before End Date.");
      return;
    }
    setLoading(true);
    try {
      const { messages } = await api.messages({
        mac_id: selectedMac || undefined,
        customer_id: selectedCustomer || undefined,
        start_date: startDate,
        end_date: endDate,
        broker: loadBroker === AUTO ? undefined : loadBroker,
      });
      setRows(messages);
      const at = new Date().toLocaleString();
      setLoadedAt(at);
      if (messages.length) {
        setMetrics({
          total: messages.length,
          brokers: new Set(messages.map((m) => m.broker_name)).size,
          topics: new Set(messages.map((m) => m.topic)).size,
          mac: selectedMac || selectedCustomer,
        });
      } else {
        setMetrics(null);
      }
    } catch (err) {
      if (err.status === 401) return onLogout();
      setNotice(err.message);
    } finally {
      setLoading(false);
    }
  }

  async function doPublish(message) {
    if (!publishTopic) {
      setPublishMsg("⚠️ No target topic — load a device first.");
      return;
    }
    if (!message.trim()) {
      setPublishMsg("⚠️ Enter a message before sending.");
      return;
    }
    try {
      const { broker } = await api.publish({
        topic: publishTopic,
        message: message.trim(),
        broker: actionBroker === AUTO ? undefined : actionBroker,
      });
      setPublishMsg(`✅ \`${message.trim()}\` sent via ${broker}`);
    } catch (err) {
      if (err.status === 401) return onLogout();
      setPublishMsg(`❌ Failed: ${err.message}`);
    }
  }

  function downloadCsv() {
    if (!rows || !rows.length) return;
    const header = ["S.No", "Broker", "Topic", "MAC ID", "Payload", "Timestamp (IST)"];
    const lines = [header.join(",")];
    rows.forEach((r, i) => {
      const cells = [
        i + 1,
        r.broker_name,
        r.topic,
        r.mac_id,
        r.payload,
        r.timestamp,
      ].map((c) => `"${String(c ?? "").replace(/"/g, '""')}"`);
      lines.push(cells.join(","));
    });
    const blob = new Blob([lines.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${String(metrics?.mac || "history").replace(/:/g, "")}_${startDate}_to_${endDate}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  const targetMac = selectedMac || (rows && rows.length ? rows[0].mac_id : null);

  return (
    <div className="dash">
      <header className="dash-header">
        <h1>📡 Flosenso MQTT Dashboard</h1>
        <div className="dash-header-right">
          <span className="user-chip">{user}</span>
          <button className="btn btn-primary" onClick={onLogout}>
            🚪 Logout
          </button>
        </div>
      </header>

      <div className="metric-row">
        {brokerCounts.map((bc) => (
          <div className="metric-card" key={bc.broker_name}>
            <div className="metric-label">📨 {bc.broker_name}</div>
            <div className="metric-value">{bc.total.toLocaleString()} msgs</div>
          </div>
        ))}
      </div>

      <h2>Device Message History</h2>
      <div className="filters">
        <label>
          Customer ID
          <select value={customer} onChange={(e) => onCustomerChange(e.target.value)}>
            <option>{PICK_CUSTOMER}</option>
            {devices.customers.map((c) => (
              <option key={c} value={c}>
                {c}
              </option>
            ))}
          </select>
        </label>
        <label>
          MAC ID
          <select value={mac} onChange={(e) => onMacChange(e.target.value)}>
            <option>{PICK_MAC}</option>
            {macOptions.map((m) => (
              <option key={m} value={m}>
                {m}
              </option>
            ))}
          </select>
        </label>
        <label>
          Start Date
          <input type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)} />
        </label>
        <label>
          End Date
          <input type="date" value={endDate} onChange={(e) => setEndDate(e.target.value)} />
        </label>
        <label>
          Load via broker
          <select value={loadBroker} onChange={(e) => setLoadBroker(e.target.value)}>
            <option>{AUTO}</option>
            {brokers.map((b) => (
              <option key={b} value={b}>
                {b}
              </option>
            ))}
          </select>
        </label>
        <button className="btn btn-primary load-btn" onClick={loadData} disabled={loading}>
          {loading ? "Loading…" : "🔄 Load Data"}
        </button>
      </div>

      {notice && <div className="alert alert-warn">{notice}</div>}

      {rows === null && (
        <div className="alert alert-info">
          👆 Choose Customer ID / MAC ID, a date range, then click <b>Load Data</b>.
        </div>
      )}

      {rows !== null && rows.length === 0 && (
        <div className="alert alert-info">
          No messages found for the selected filters. {loadedAt && `Loaded at ${loadedAt}.`}
        </div>
      )}

      {rows && rows.length > 0 && (
        <>
          <div className="metric-row">
            <div className="metric-card">
              <div className="metric-label">Total Records</div>
              <div className="metric-value">{metrics.total.toLocaleString()}</div>
            </div>
            <div className="metric-card">
              <div className="metric-label">Brokers Seen</div>
              <div className="metric-value">{metrics.brokers}</div>
            </div>
            <div className="metric-card">
              <div className="metric-label">Unique Topics</div>
              <div className="metric-value">{metrics.topics}</div>
            </div>
          </div>
          <div className="table-caption">
            Showing <b>{metrics.total.toLocaleString()}</b> record(s) for{" "}
            <code>{metrics.mac}</code> · loaded {loadedAt}
            <button className="btn btn-ghost" onClick={downloadCsv}>
              ⬇️ Download CSV
            </button>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>S.No</th>
                  <th>Broker</th>
                  <th>Topic</th>
                  <th>MAC ID</th>
                  <th>Payload</th>
                  <th>Timestamp (IST)</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r, i) => (
                  <tr key={i}>
                    <td>{i + 1}</td>
                    <td>{r.broker_name}</td>
                    <td>{r.topic}</td>
                    <td>{r.mac_id}</td>
                    <td className="payload-cell">{r.payload}</td>
                    <td>{r.timestamp}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      {targetMac && (
        <section className="quick-actions">
          <h2>⚡ Quick Actions</h2>
          <div className="qa-controls">
            <label>
              Send via broker
              <select value={actionBroker} onChange={(e) => setActionBroker(e.target.value)}>
                <option>{AUTO}</option>
                {brokers.map((b) => (
                  <option key={b} value={b}>
                    {b}
                  </option>
                ))}
              </select>
            </label>
            <div className="qa-target">
              📡 Publishing to: <b>{publishTopic}</b> · Broker: <b>{actionBroker}</b>
            </div>
          </div>

          <div className="qa-grid">
            {commands.map((c) => (
              <button key={c.label} className="btn btn-cmd" onClick={() => doPublish(c.cmd)}>
                {c.label}
              </button>
            ))}
          </div>

          <div className="qa-custom">
            <input
              type="text"
              placeholder="e.g.  app210&MO&10"
              value={customMsg}
              onChange={(e) => setCustomMsg(e.target.value)}
            />
            <button className="btn btn-primary" onClick={() => doPublish(customMsg)}>
              📤 Send Custom
            </button>
          </div>
          {publishMsg && <div className="alert alert-info">{publishMsg}</div>}
        </section>
      )}
    </div>
  );
}
