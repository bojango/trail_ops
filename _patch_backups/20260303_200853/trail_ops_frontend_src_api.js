// Central API client for TrailOps frontend

const DEFAULT_API_BASE = "http://127.0.0.1:8000";

export function apiBase() {
  const env = (import.meta && import.meta.env) ? import.meta.env : {};
  const base = env.VITE_API_BASE || DEFAULT_API_BASE;
  return String(base).replace(/\/$/, "");
}

function url(path) {
  const p = String(path || "");
  if (p.startsWith("http://") || p.startsWith("https://")) return p;
  return `${apiBase()}${p.startsWith("/") ? "" : "/"}${p}`;
}

async function httpJson(path, opts = {}) {
  const res = await fetch(url(path), {
    ...opts,
    headers: {
      "Accept": "application/json",
      ...(opts.headers || {}),
    },
  });

  const text = await res.text();
  const data = text ? (() => {
    try { return JSON.parse(text); } catch { return { raw: text }; }
  })() : null;

  if (!res.ok) {
    const msg = (data && (data.detail || data.error)) ? (data.detail || data.error) : `${res.status} ${res.statusText}`;
    throw new Error(msg);
  }
  return data;
}

function qs(params) {
  const sp = new URLSearchParams();
  Object.entries(params || {}).forEach(([k, v]) => {
    if (v === undefined || v === null || v === "") return;
    sp.set(k, String(v));
  });
  const s = sp.toString();
  return s ? `?${s}` : "";
}

export async function fetchHealth() {
  return httpJson("/health");
}

export async function fetchWorkouts({ start, end, sport = "all", limit = 300 } = {}) {
  return httpJson(`/workouts${qs({ start, end, sport, limit })}`);
}

export async function fetchWorkoutContext(workoutId) {
  return httpJson(`/workouts/${encodeURIComponent(String(workoutId))}/context`);
}

export async function fetchWorkoutMapPoints(workoutId, { level = 0, max_points = 12000, include_markers = true } = {}) {
  return httpJson(`/workouts/${encodeURIComponent(String(workoutId))}/map-points${qs({ level, max_points, include_markers })}`);
}

export async function fetchSparklines(payload) {
  return httpJson("/workouts/sparklines", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
}

export async function fetchPeaksDashboard(range = "30d", cls = "wainwrights") {
  return httpJson(`/peaks/dashboard${qs({ range, cls })}`);
}

// Diagnostics: fetch details for a single peak/POI.
// Backend may implement either /peaks/item?peak_osm_id=... or /peaks/item/<id>.
export async function fetchPeakItem(peakOsmId, kind = "peak") {
  const id = encodeURIComponent(String(peakOsmId));
  const k = encodeURIComponent(String(kind || "poi"));
  try {
    return await httpJson(`/peaks/item${qs({ peak_osm_id: peakOsmId, kind })}`);
  } catch (e) {
    return await httpJson(`/peaks/item/${id}?kind=${k}`);
  }
}
