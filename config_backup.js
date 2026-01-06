const API_CANDIDATES = [
  "https://nga-postauditory-unharmonically.ngrok-free.dev",
  "http://localhost:5005",
  "http://127.0.0.1:5005"
];

async function sha256(text) {
  const enc = new TextEncoder().encode(text);
  const hashBuffer = await crypto.subtle.digest("SHA-256", enc);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map((b) => b.toString(16).padStart(2, "0")).join("");
}

function readStore(key, def) {
  try {
    const v = localStorage.getItem(key);
    return v ? JSON.parse(v) : def;
  } catch (_) {
    return def;
  }
}
function writeStore(key, obj) {
  try {
    localStorage.setItem(key, JSON.stringify(obj));
  } catch (_) { }
}

function getQueryParam(name) {
  const params = new URLSearchParams(window.location.search);
  return params.get(name);
}

function randomHex(len) {
  const arr = new Uint8Array(len);
  crypto.getRandomValues(arr);
  return Array.from(arr).map((b) => b.toString(16).padStart(2, "0")).join("");
}
function getOfflineUsers() {
  return readStore("offlineUsers", {});
}
function setOfflineUsers(users) {
  writeStore("offlineUsers", users);
}
function getOfflineQueue() {
  return readStore("offlineQueue", []);
}
function setOfflineQueue(q) {
  writeStore("offlineQueue", q);
}
async function offlineRegisterUser(name, matricula, password) {
  const users = getOfflineUsers();
  if (users[matricula]) return { ok: false, message: "Matrícula já existe offline" };
  const salt = randomHex(16);
  const hashed = await sha256(password + salt);
  users[matricula] = { name, role: "user", salt, hash: hashed };
  setOfflineUsers(users);
  return { ok: true, message: "Cadastro offline realizado" };
}
async function offlineLogin(matricula, password) {
  const users = getOfflineUsers();
  const u = users[matricula];
  if (!u) return null;
  const hashed = await sha256(password + u.salt);
  if (hashed !== u.hash) return null;
  return { token: `offline:${matricula}`, role: u.role || "user", name: u.name || matricula };
}
function offlinePunchAdd(type, neighborhood, city) {
  const q = getOfflineQueue();
  const ts = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  const tsStr = `${ts.getFullYear()}-${pad(ts.getMonth() + 1)}-${pad(ts.getDate())} ${pad(ts.getHours())}:${pad(ts.getMinutes())}:${pad(ts.getSeconds())}`;
  q.push({ type, timestamp: tsStr, neighborhood, city });
  setOfflineQueue(q);
  return { ok: true };
}
function offlineHistory() {
  const q = getOfflineQueue();
  const now = new Date();
  const currentMonth = now.getMonth(); // 0-11
  const currentYear = now.getFullYear();

  const filtered = q.filter(r => {
    // timestamp format: YYYY-MM-DD HH:MM:SS
    // We can try to parse it, or just compare substrings
    try {
      // Replace space with T to make it ISO-like for safer parsing if needed, 
      // but YYYY-MM-DD HH:MM:SS usually parses in modern browsers or we can simple parse strings.
      const parts = r.timestamp.split(' ')[0].split('-');
      const rYear = parseInt(parts[0]);
      const rMonth = parseInt(parts[1]) - 1; // 0-based
      return rYear === currentYear && rMonth === currentMonth;
    } catch (e) {
      return true; // Keep if parse fails to be safe
    }
  });

  return filtered.map((r) => ({ type: r.type, timestamp: r.timestamp, neighborhood: r.neighborhood, city: r.city, pending: true }));
}
async function offlineSync(token) {
  const q = getOfflineQueue();
  let migrated = 0;
  const remain = [];
  for (const r of q) {
    try {
      const res = await apiFetch(`/api/punch`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` },
        body: JSON.stringify({ type: r.type, neighborhood: r.neighborhood, city: r.city }),
      });
      if (res.ok) migrated += 1;
      else remain.push(r);
    } catch (_) {
      remain.push(r);
    }
  }
  setOfflineQueue(remain);
  return { migrated };
}
async function isApiAvailable() {
  try {
    await window.API_READY;
    const res = await apiFetch(`/api/online`, { method: "GET" });
    const js = await res.json();
    return !!js.online;
  } catch (_) {
    return false;
  }
}

async function detectApiBase() {
  const cached = readStore("api_base", null);
  const override = readStore("api_override", null);
  const order = [];
  if (override) order.push(override);
  if (cached && cached !== override) order.push(cached);
  order.push(...API_CANDIDATES.filter((b) => b !== cached));
  for (const base of order) {
    try {
      const res = await fetchWithTimeout(`${base}/api/online`, { method: "GET" }, 3000);
      if (res.ok) {
        writeStore("api_base", base);
        return base;
      }
    } catch (_) { }
  }
  const fallback = API_CANDIDATES[0];
  writeStore("api_base", fallback);
  return fallback;
}

async function apiFetch(path, options) {
  const bases = [];
  const cached = readStore("api_base", null);
  const override = readStore("api_override", null);
  if (override) bases.push(override);
  if (window.API_BASE_URL) bases.push(window.API_BASE_URL);
  if (cached && !bases.includes(cached)) bases.push(cached);
  bases.push(...API_CANDIDATES.filter((b) => !bases.includes(b)));
  for (const base of bases) {
    try {
      const res = await fetchWithTimeout(`${base}${path}`, options || {}, 5000);
      if (res.ok || res.status) {
        window.API_BASE_URL = base;
        writeStore("api_base", base);
        return res;
      }
    } catch (_) {
      continue;
    }
  }
  throw new Error("API indisponível");
}

function fetchWithTimeout(resource, options, timeoutMs) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs || 5000);
  return fetch(resource, { ...(options || {}), signal: controller.signal }).finally(() => clearTimeout(id));
}

const initialApi = getQueryParam("api") || getQueryParam("api_base");
if (initialApi) {
  writeStore("api_override", initialApi);
  writeStore("api_base", initialApi);
  window.API_BASE_URL = initialApi;
}
window.API_READY = detectApiBase().then((b) => {
  window.API_BASE_URL = b;
  writeStore("api_base", b);
  window.apiFetch = apiFetch;
  window.sha256 = sha256;
  window.readStore = readStore;
  window.writeStore = writeStore;
  window.offline = {
    registerUser: offlineRegisterUser,
    login: offlineLogin,
    punchAdd: offlinePunchAdd,
    history: offlineHistory,
    sync: offlineSync,
    isApiAvailable,
  };
  window.setApiEndpoint = function (url) {
    writeStore("api_override", url);
    writeStore("api_base", url);
    window.API_BASE_URL = url;
  };
  return b;
});
