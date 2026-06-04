import React, { useState, useCallback } from "react";
import Login from "./pages/Login.jsx";
import Dashboard from "./pages/Dashboard.jsx";
import { isAuthenticated, getUser, clearSession } from "./auth.js";

export default function App() {
  const [authed, setAuthed] = useState(isAuthenticated());
  const [user, setUser] = useState(getUser());

  const onLogin = useCallback((username) => {
    setUser(username);
    setAuthed(true);
  }, []);

  const onLogout = useCallback(() => {
    clearSession();
    setUser("");
    setAuthed(false);
  }, []);

  if (!authed) {
    return <Login onLogin={onLogin} />;
  }
  return <Dashboard user={user} onLogout={onLogout} />;
}
