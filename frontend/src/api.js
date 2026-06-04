// Tiny fetch wrapper around the FastAPI backend. No auth header: login is
// handled entirely in the frontend (see auth.js); the API is open.

// Dev: Vite proxies /api → backend (vite.config.js). Prod: direct URL.
const API_BASE = import.meta.env.DEV ? "" : "http://3.7.113.12:8500";

async function request(path, { method = "GET", body } = {}) {
  const headers = { "Content-Type": "application/json" };
  const url = `${API_BASE}/api${path}`;
  const res = await fetch(url, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });

  let data = null;
  try {
    data = await res.json();
  } catch {
    /* empty body */
  }

  if (!res.ok) {
    const detail = (data && data.detail) || res.statusText;
    throw new ApiError(detail, res.status);
  }
  return data;
}

export class ApiError extends Error {
  constructor(message, status) {
    super(message);
    this.status = status;
  }
}

export const api = {
  brokers: () => request("/brokers"),
  brokerCounts: () => request("/stats/broker-counts"),
  devices: () => request("/devices"),
  commands: () => request("/commands"),
  messages: (params) => {
    const qs = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null && v !== "") qs.append(k, v);
    });
    return request(`/messages?${qs.toString()}`);
  },
  publish: (payload) => request("/publish", { method: "POST", body: payload }),
};
