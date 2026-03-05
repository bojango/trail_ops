const DEFAULT_BASE = "http://127.0.0.1:8000";

export function apiBase() {
  // Allow override via Vite env, but keep local-first default.
  return import.meta.env?.VITE_API_BASE || DEFAULT_BASE;
}

export async function fetchHealth() {
  const r = await fetch(`${apiBase()}/health`, { method: "GET" });
  if (!r.ok) throw new Error(`health ${r.status}`);
  return await r.json();
}

export async function fetchWorkouts({ start, end, sport = "all", limit = 300 } = {}) {
  const params = new URLSearchParams();
  if (start) params.set("start", start);
  if (end) params.set("end", end);
  if (sport && sport !== "all") params.set("sport", sport);
  if (limit) params.set("limit", String(limit));

  const url = `${apiBase()}/workouts?${params.toString()}`;
  const r = await fetch(url, { method: "GET" });
  if (!r.ok) throw new Error(`workouts ${r.status}`);
  return await r.json();
}
