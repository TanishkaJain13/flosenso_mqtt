import React, { useEffect, useMemo, useState, useCallback } from "react";
import { api } from "../api.js";
import { CC_USER, CC_HIDDEN_COMMANDS } from "../auth.js";
import SearchableSelect from "../components/SearchableSelect.jsx";

const AUTO = "Auto (try all)";

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

  // Selectors ("" = nothing selected)
  const [customer, setCustomer] = useState("");
  const [mac, setMac] = useState("");
  const [startDate, setStartDate] = useState(isoDate(daysAgo(30)));
  const [endDate, setEndDate] = useState(isoDate(new Date()));
  const [loadBroker, setLoadBroker] = useState(AUTO);

  // Results
  const [rows, setRows] = useState(null); // null = nothing loaded yet
  const [metrics, setMetrics] = useState(null);
  const [loadedAt, setLoadedAt] = useState(null);
  const [loading, setLoading] = useState(false);
  const [notice, setNotice] = useState("");
  const [tableSearch, setTableSearch] = useState(""); // client-side table filter

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
    if (customer) return devices.macs_by_customer[customer] || [];
    return devices.macs;
  }, [customer, devices]);

  // Selecting a MAC auto-fills its customer (mirrors the Streamlit behaviour).
  function onMacChange(value) {
    setMac(value);
    if (value && !customer) {
      const owner = Object.entries(devices.macs_by_customer).find(([, macs]) =>
        macs.includes(value)
      );
      if (owner) setCustomer(owner[0]);
    }
  }

  function onCustomerChange(value) {
    setCustomer(value);
    // Reset MAC if it no longer belongs to the selected customer.
    const allowed = value ? devices.macs_by_customer[value] || [] : devices.macs;
    if (mac && !allowed.includes(mac)) setMac("");
  }

  const selectedMac = mac || null;
  const selectedCustomer = customer || null;

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

  // Client-side table search: filters the already-loaded rows across all columns.
  const filteredRows = useMemo(() => {
    if (!rows) return [];
    const q = tableSearch.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter((r) =>
      [r.broker_name, r.topic, r.mac_id, r.payload, r.timestamp]
        .some((v) => String(v ?? "").toLowerCase().includes(q))
    );
  }, [rows, tableSearch]);

  function downloadCsv() {
    if (!filteredRows.length) return;
    const header = ["S.No", "Broker", "Topic", "MAC ID", "Payload", "Timestamp (IST)"];
    const lines = [header.join(",")];
    filteredRows.forEach((r, i) => {
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
          <SearchableSelect
            value={customer}
            onChange={onCustomerChange}
            options={devices.customers}
            placeholder="Search Customer ID…"
          />
        </label>
        <label>
          MAC ID
          <SearchableSelect
            value={mac}
            onChange={onMacChange}
            options={macOptions}
            placeholder="Search MAC ID…"
          />
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
          <div className="table-toolbar">
            <input
              type="search"
              className="table-search"
              placeholder="🔍 Search table (topic, MAC, payload, broker, time)…"
              value={tableSearch}
              onChange={(e) => setTableSearch(e.target.value)}
            />
            <span className="table-caption">
              Showing <b>{filteredRows.length.toLocaleString()}</b>
              {tableSearch.trim() && <> of {metrics.total.toLocaleString()}</>} record(s)
              {!tableSearch.trim() && (
                <>
                  {" "}for <code>{metrics.mac}</code>
                </>
              )}{" "}
              · loaded {loadedAt}
            </span>
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
                {filteredRows.length === 0 ? (
                  <tr>
                    <td colSpan={6} className="table-empty">
                      No rows match “{tableSearch}”.
                    </td>
                  </tr>
                ) : (
                  filteredRows.map((r, i) => (
                    <tr key={i}>
                      <td>{i + 1}</td>
                      <td>{r.broker_name}</td>
                      <td>{r.topic}</td>
                      <td>{r.mac_id}</td>
                      <td className="payload-cell">{r.payload}</td>
                      <td>{r.timestamp}</td>
                    </tr>
                  ))
                )}
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
