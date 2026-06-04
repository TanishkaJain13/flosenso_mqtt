// Static, frontend-only login. No backend token / JWT: credentials are checked
// against the table below and the result is kept in localStorage. Note that
// frontend-only auth is not a security boundary — anyone can read these values
// in the bundle. It only gates the UI.

const STATIC_USERS = {
  admin: "admin123",
  "flosenso.cc@hipl.co.in": "flosenso@cc123",
};

// Commands hidden from the restricted flosenso.cc account.
export const CC_USER = "flosenso.cc@hipl.co.in";
export const CC_HIDDEN_COMMANDS = ["📏 CHECK_DISTANCE", "🔄 RESET_LORA"];

const AUTH_KEY = "flosenso_auth";
const USER_KEY = "flosenso_user";

/** Validate credentials locally. Returns the username on success, else null. */
export function staticLogin(username, password) {
  const u = (username || "").trim();
  if (STATIC_USERS[u] !== undefined && STATIC_USERS[u] === password) {
    return u;
  }
  return null;
}

export function isAuthenticated() {
  return localStorage.getItem(AUTH_KEY) === "1";
}

export function getUser() {
  return localStorage.getItem(USER_KEY) || "";
}

export function setSession(username) {
  localStorage.setItem(AUTH_KEY, "1");
  localStorage.setItem(USER_KEY, username);
}

export function clearSession() {
  localStorage.removeItem(AUTH_KEY);
  localStorage.removeItem(USER_KEY);
}
