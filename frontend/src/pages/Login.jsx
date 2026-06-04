import React, { useState } from "react";
import { staticLogin, setSession } from "../auth.js";

export default function Login({ onLogin }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");

  function submit(e) {
    e.preventDefault();
    setError("");
    const user = staticLogin(username, password);
    if (user) {
      setSession(user);
      onLogin(user);
    } else {
      setError("Invalid credentials");
    }
  }

  return (
    <div className="login-page">
      <div className="login-brand">
        <h1>📡 Flosenso</h1>
        <p>Server Management Dashboard</p>
      </div>
      <form className="login-card" onSubmit={submit}>
        <h3>🔐 Admin Login</h3>
        <label>
          Username
          <input
            type="text"
            placeholder="Enter username"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            autoFocus
          />
        </label>
        <label>
          Password
          <input
            type="password"
            placeholder="Enter password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />
        </label>
        {error && <div className="alert alert-error">{error}</div>}
        <button type="submit" className="btn btn-primary">
          Sign In
        </button>
      </form>
    </div>
  );
}
