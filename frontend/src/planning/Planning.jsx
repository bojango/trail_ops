import React, { useEffect, useState } from "react";
import { planningListRoutes } from "./planning_api";

const nf1 = new Intl.NumberFormat("en-GB", { maximumFractionDigits: 1, minimumFractionDigits: 1 });
const nf0 = new Intl.NumberFormat("en-GB", { maximumFractionDigits: 0 });

export default function Planning() {
  const [routes, setRoutes] = useState([]);
  const [err, setErr] = useState(null);
  const [loading, setLoading] = useState(false);

  async function load() {
    setErr(null);
    setLoading(true);
    try {
      const data = await planningListRoutes();
      setRoutes(Array.isArray(data) ? data : []);
    } catch (e) {
      setErr(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { load(); }, []);

  return (
    <section className="panel span2" style={{ marginTop: 14 }}>
      <div className="panelHdr">
        <div className="panelTitle">PLANNING</div>
        <div className="panelSub">Route Library (Phase 3). Map + upload UI next.</div>
      </div>

      <div style={{ padding: 12 }}>
        <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 10 }}>
          <button className="pillBtn" onClick={load} disabled={loading}>
            {loading ? "Loading..." : "Refresh"}
          </button>
          {err && <div className="mutedLine" style={{ color: "#ff6b6b" }}>{err}</div>}
        </div>

        <div className="list scroll" style={{ maxHeight: 420 }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr className="mutedLine" style={{ textAlign: "left" }}>
                <th style={{ padding: "8px 6px" }}>Name</th>
                <th style={{ padding: "8px 6px" }}>Distance</th>
                <th style={{ padding: "8px 6px" }}>Ascent</th>
              </tr>
            </thead>
            <tbody>
              {routes.map((r) => (
                <tr key={r.route_id} style={{ borderTop: "1px solid rgba(255,255,255,0.08)" }}>
                  <td style={{ padding: "8px 6px" }}>{r.name}</td>
                  <td style={{ padding: "8px 6px" }}>
                    {nf1.format((Number(r.distance_m) || 0) / 1000)} km
                  </td>
                  <td style={{ padding: "8px 6px" }}>
                    {nf0.format(Number(r.ascent_m) || 0)} m
                  </td>
                </tr>
              ))}
              {routes.length === 0 && !loading && (
                <tr>
                  <td colSpan="3" style={{ padding: "10px 6px" }} className="mutedLine">
                    No routes yet. Upload one via the API (Phase 2) and hit Refresh.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}
