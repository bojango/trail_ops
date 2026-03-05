import React, { useEffect, useMemo, useState } from "react";
import { fetchPeaksDashboard, fetchPeakItem, renamePeakItem } from "../api";

const nf0 = new Intl.NumberFormat("en-GB", { maximumFractionDigits: 0 });

const PEAK_CLASSES = [
  { value: "wainwrights", label: "Wainwrights" },
  { value: "dodds", label: "Dodds" },
  { value: "birkett", label: "Birketts" },
  { value: "hewitts", label: "Hewitts" },
  { value: "nuttalls", label: "Nuttalls" },
  { value: "munros", label: "Munros" },
  { value: "corbetts", label: "Corbetts" },
  { value: "grahams", label: "Grahams" },
  { value: "marilyns", label: "Marilyns" },
];

function ymdToDate(ymd) {
  // Force local midnight to avoid DST weirdness in comparisons.
  const s = String(ymd || "").slice(0, 10);
  return new Date(`${s}T00:00:00`);
}

function bucket(n) {
  const v = Number(n) || 0;
  if (v <= 0) return 0;
  if (v === 1) return 1;
  if (v === 2) return 2;
  return 3; // 3+
}

export default function PeaksPanel() {
  const [peaksSummaryMode, setPeaksSummaryMode] = useState("ai");
  const [peaksRange, setPeaksRange] = useState("30d");
  const [peaksClass, setPeaksClass] = useState("wainwrights");

  const [peaksDash, setPeaksDash] = useState(null);
  const [peaksDashErr, setPeaksDashErr] = useState(null);
  const [peaksDashLoading, setPeaksDashLoading] = useState(false);

  // Diagnostics drawer
  const [selectedItem, setSelectedItem] = useState(null); // { id, kind }
  const [peakItem, setPeakItem] = useState(null);
  const [peakItemLoading, setPeakItemLoading] = useState(false);
  const [peakItemErr, setPeakItemErr] = useState(null);

  const [renameText, setRenameText] = useState("");
  const [renameSaving, setRenameSaving] = useState(false);
  const [renameMsg, setRenameMsg] = useState("");

  // Peaks dashboard aggregates (global)
  useEffect(() => {
    let alive = true;

    async function loadPeaksDash() {
      setPeaksDashErr(null);
      setPeaksDashLoading(true);
      try {
        // IMPORTANT: api.js signature is (range, cls) not an object.
        const data = await fetchPeaksDashboard(peaksRange, peaksClass);
        if (!alive) return;
        setPeaksDash(data);
      } catch (e) {
        if (!alive) return;
        setPeaksDash(null);
        setPeaksDashErr(String(e?.message || e));
      } finally {
        if (alive) setPeaksDashLoading(false);
      }
    }

    loadPeaksDash();
    return () => { alive = false; };
  }, [peaksRange, peaksClass]);

  // Peak/POI diagnostics (click-through)
  useEffect(() => {
    let alive = true;
    async function loadPeakItem() {
      if (!selectedItem?.id) { setPeakItem(null); setPeakItemErr(null); setRenameText(""); setRenameMsg(""); return; }
      setPeakItemErr(null);
      setPeakItemLoading(true);
      setRenameMsg("");
      try {
        const data = await fetchPeakItem(selectedItem.id, selectedItem.kind || "peak");
        if (!alive) return;
        setPeakItem(data);
        setRenameText(String((data && data.name) || ""));
      } catch (e) {
        if (!alive) return;
        setPeakItem(null);
        setPeakItemErr(String(e?.message || e));
      } finally {
        if (alive) setPeakItemLoading(false);
      }
    }
    loadPeakItem();
    return () => { alive = false; };
  }, [selectedItem?.id, selectedItem?.kind]);

  const peaksStats = peaksDash?.stats || {};
  const peaksTrackers =
  Array.isArray(peaksDash?.class_trackers)
    ? peaksDash.class_trackers
    : (Array.isArray(peaksDash?.classTrackers) ? peaksDash.classTrackers : []);
  const peaksTopVisited = Array.isArray(peaksDash?.top_visited) ? peaksDash.top_visited : [];
  const peaksTopVisitedPois = Array.isArray(peaksDash?.top_visited_pois) ? peaksDash.top_visited_pois : [];
  const poiKinds = Array.isArray(peaksDash?.poi_kinds) ? peaksDash.poi_kinds : [];
  const nearMissPeaks = Array.isArray(peaksDash?.near_misses?.peaks) ? peaksDash.near_misses.peaks : [];
  const nearMissPois = Array.isArray(peaksDash?.near_misses?.pois) ? peaksDash.near_misses.pois : [];

  const primaryTracker = useMemo(() => {
    if (!peaksTrackers.length) return null;
    return peaksTrackers.find((t) => String(t.key) === String(peaksClass)) || peaksTrackers[0];
  }, [peaksTrackers, peaksClass]);

  const secondaryTracker = useMemo(() => {
    if (!peaksTrackers.length) return null;
    const primaryKey = primaryTracker?.key;
    // Prefer another tracker with some progress.
    const candidates = peaksTrackers.filter((t) => t.key !== primaryKey);
    const progressed = candidates.filter((t) => (Number(t.done) || 0) > 0);
    return progressed[0] || candidates[0] || null;
  }, [peaksTrackers, primaryTracker]);

  function trackerMood(pct) {
    if (pct >= 55) return "Strong momentum";
    if (pct >= 25) return "Solid progress";
    if (pct > 0) return "Warming up";
    return "Not started";
  }

  const heat = useMemo(() => {
    const days = Array.isArray(peaksDash?.heatmap?.days) ? peaksDash.heatmap.days : [];

    const byDay = new Map();
    for (const d of days) {
      if (d?.day) byDay.set(String(d.day).slice(0, 10), Number(d.count) || 0);
    }

    // Backend provides last 30 days ending today (inclusive). We render exactly those days,
    // but align to Monday so headers match columns, padding leading/trailing blanks.
    const dayKeys = days.map((d) => String(d?.day || "").slice(0, 10)).filter(Boolean);
    const startKey = dayKeys[0] || null;

    const startDt = startKey ? ymdToDate(startKey) : (() => {
      const t = new Date();
      t.setHours(0, 0, 0, 0);
      t.setDate(t.getDate() - 29);
      return t;
    })();

    const startMon0 = (startDt.getDay() + 6) % 7; // Mon=0 .. Sun=6
    const padStart = startMon0;

    const totalCells = padStart + (dayKeys.length || 30);
    const rows = Math.ceil(totalCells / 7);

    const cells = [];
    // Build sequential dates from startKey (or startDt fallback) for dayKeys length.
    const nDays = dayKeys.length || 30;
    for (let i = 0; i < nDays; i++) {
      const dt = new Date(startDt.getTime() + i * 24 * 3600 * 1000);
      const y = dt.getFullYear();
      const m = String(dt.getMonth() + 1).padStart(2, "0");
      const d = String(dt.getDate()).padStart(2, "0");
      const key = `${y}-${m}-${d}`;
      const c = byDay.get(key) || 0;
      cells.push({ key, count: c, b: bucket(c) });
    }

    const weeks = [];
    for (let r = 0; r < rows; r++) {
      const row = [];
      for (let c = 0; c < 7; c++) {
        const idx = r * 7 + c;
        const dayIdx = idx - padStart;
        if (dayIdx < 0 || dayIdx >= cells.length) {
          row.push({ blank: true, b: 0, title: "" });
        } else {
          const cell = cells[dayIdx];
          row.push({ blank: false, b: cell.b, title: `${cell.key} • ${cell.count} hits` });
        }
      }
      weeks.push(row);
    }

    return { weeks, padStart, rows };
  }, [peaksDash]);

  function openDiag(id, kind = "peak") {
    if (id == null) return;
    setSelectedItem({ id, kind: kind || "peak" });
  }

  function closeDiag() {
    setSelectedItem(null);
  }

  async function onSaveRename() {
    if (!selectedItem?.id) return;
    const nextName = String(renameText || "").trim();
    if (!nextName) {
      setRenameMsg("Name required");
      return;
    }
    setRenameSaving(true);
    setRenameMsg("");
    try {
      const res = await renamePeakItem(selectedItem.id, nextName, selectedItem.kind || "peak");
      setPeakItem((prev) => (prev ? { ...prev, name: nextName } : prev));
      setRenameMsg(res?.ok === false ? (res?.error || "Rename failed") : "Saved");
    } catch (e) {
      setRenameMsg(String(e?.message || e || "Rename failed"));
    } finally {
      setRenameSaving(false);
    }
  }

  const diagOpen = Boolean(selectedItem?.id);

  return (
    <div className="panel span2 peaksPanel">
      <div className="panelHdr panelHdrRow">
        <div>
          <div className="panelTitle">PEAKS • LISTS • PROGRESS</div>
          <div className="panelSub">Stable Peaks UI wiring: heatmap, POIs, trackers, lists, diagnostics.</div>
        </div>
        <div className="panelControls">
          <select className="sel" value={peaksSummaryMode} onChange={(e) => setPeaksSummaryMode(e.target.value)}>
            <option value="ai">AI SUMMARY</option>
            <option value="manual">MANUAL</option>
          </select>
          <select className="sel" value={peaksRange} onChange={(e) => setPeaksRange(e.target.value)}>
            <option value="7d">RANGE 7D</option>
            <option value="30d">RANGE 30D</option>
            <option value="12m">RANGE 12M</option>
            <option value="all">RANGE ALL</option>
          </select>
          <select className="sel" value={peaksClass} onChange={(e) => setPeaksClass(e.target.value)}>
            {PEAK_CLASSES.map((c) => (
              <option key={c.value} value={c.value}>
                CLASS {c.label.toUpperCase()}
              </option>
            ))}
          </select>
        </div>
      </div>

      <div className="peaksTopRow">
        <div className="miniCard">
          <div className="miniK">PEAKS BAGGED</div>
          <div className="miniV">{Number.isFinite(Number(peaksStats.peaks_bagged)) ? nf0.format(Number(peaksStats.peaks_bagged)) : "—"}</div>
          <div className="miniS">{peaksRange === "12m" ? "12m" : peaksRange}</div>
        </div>
        <div className="miniCard">
          <div className="miniK">UNIQUE PEAKS</div>
          <div className="miniV">{Number.isFinite(Number(peaksStats.unique_peaks)) ? nf0.format(Number(peaksStats.unique_peaks)) : "—"}</div>
          <div className="miniS">{(peaksRange === "12m" ? "12m" : peaksRange) + (Number.isFinite(Number(peaksStats.repeats)) ? ` • ${nf0.format(Number(peaksStats.repeats))} repeats` : "")}</div>
        </div>
        <div className="miniCard">
          <div className="miniK">LIFETIME UNIQUE</div>
          <div className="miniV">{Number.isFinite(Number(peaksStats.lifetime_unique)) ? nf0.format(Number(peaksStats.lifetime_unique)) : "—"}</div>
          <div className="miniS">tracked</div>
        </div>
        <div className="miniCard">
          <div className="miniK">PEAK RATE</div>
          <div className="miniV">{Number.isFinite(Number(peaksStats.peak_rate_per_week)) ? Number(peaksStats.peak_rate_per_week).toFixed(2) : "—"}</div>
          <div className="miniS">/wk • {peaksStats.rate_window || "rolling"}</div>
        </div>
      </div>

      <div className="peaksBody">
        <div className="peaksLeft">
          <div className="subPanel">
            <div className="subHdr">PEAK HITS • LAST 30 DAYS</div>
            <div className="dowHdr">
              {["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"].map((d) => (
                <div key={d} className="dowCell">{d}</div>
              ))}
                          </div>

            <div className="hitGrid">
              {heat.weeks.map((week, wi) => (
                <div key={wi} className="hitRow">
                  {week.map((cell, di) => (
                    <div
                      key={di}
                      className={`hitCell ${cell.blank ? "hitBlank" : `hit${cell.b}`}`}
                      title={cell.title}
                    />
                  ))}
                </div>
              ))}
            </div>

            <div className="legend">
              <div className="legendLabel">LEGEND</div>
              <div className="legendSwatches">
                <span className="sw sw0" /> <span className="swLab">0</span>
                <span className="sw sw1" /> <span className="swLab">1</span>
                <span className="sw sw2" /> <span className="swLab">2</span>
                <span className="sw sw3" /> <span className="swLab">3+</span>
              </div>
            </div>
          </div>

          <div className="subSection">
            <div className="topVisitedHdr">
              <div className="subHdr">POIS VISITED</div>
              <div className="subMeta">{(Number.isFinite(Number(peaksStats.pois_bagged)) ? nf0.format(Number(peaksStats.pois_bagged)) : "—")} hits • {(Number.isFinite(Number(peaksStats.unique_pois)) ? nf0.format(Number(peaksStats.unique_pois)) : "—")} unique</div>
            </div>

            <div className="pillRow">
              {poiKinds.slice(0, 8).map((k) => (
                <span key={k.kind} className="miniPill">{String(k.kind).toUpperCase()} {nf0.format(Number(k.count) || 0)}</span>
              ))}
              {!poiKinds.length && <div className="mutedLine">No POI hits in range.</div>}
            </div>

            <div className="topVisitedHdr" style={{ marginTop: 12 }}>
              <div className="subHdr">TOP VISITED • POIS</div>
              <div className="subMeta">ALL-TIME</div>
            </div>

            <div className="topVisited scrollBox">
              {peaksTopVisitedPois.map((p) => (
                <button
                  type="button"
                  key={`${p.peak_osm_id}-${p.kind}`}
                  className="topRow topRowBtn"
                  onClick={() => openDiag(p.peak_osm_id, p.kind || "poi")}
                  title="Open diagnostics"
                >
                  <div className="topName">{String(p.name || "(unknown)").toUpperCase()}</div>
                  <div className="topSub">last: {p.last || "—"} • {String(p.kind || "poi")}</div>
                  <div className="topCount">{Number.isFinite(Number(p.count)) ? nf0.format(Number(p.count)) : "—"}</div>
                </button>
              ))}
              {!peaksTopVisitedPois.length && <div className="mutedLine">No POI hits tracked yet.</div>}
            </div>

            <div className="topVisitedHdr" style={{ marginTop: 12 }}>
              <div className="subHdr">NEAR MISSES</div>
              <div className="subMeta">never bagged</div>
            </div>

            <div className="nearBox">
              <div className="nearCol">
                <div className="nearK">PEAKS</div>
                {nearMissPeaks.slice(0, 6).map((p) => (
                  <button
                    type="button"
                    key={p.peak_osm_id}
                    className="nearRow nearRowBtn"
                    onClick={() => openDiag(p.peak_osm_id, "peak")}
                    title="Open diagnostics"
                  >
                    <div className="nearName">{String(p.name || "(unknown)").toUpperCase()}</div>
                    <div className="nearSub">last: {p.last_near || "—"}</div>
                    <div className="nearCount">{nf0.format(Number(p.near_hits) || 0)}</div>
                  </button>
                ))}
                {!nearMissPeaks.length && <div className="mutedLine">None. (Or your threshold is generous.)</div>}
              </div>

              <div className="nearCol">
                <div className="nearK">POIS</div>
                {nearMissPois.slice(0, 6).map((p) => (
                  <button
                    type="button"
                    key={`${p.peak_osm_id}-${p.kind}`}
                    className="nearRow nearRowBtn"
                    onClick={() => openDiag(p.peak_osm_id, p.kind || "poi")}
                    title="Open diagnostics"
                  >
                    <div className="nearName">{String(p.name || "(unknown)").toUpperCase()}</div>
                    <div className="nearSub">last: {p.last_near || "—"} • {String(p.kind || "poi")}</div>
                    <div className="nearCount">{nf0.format(Number(p.near_hits) || 0)}</div>
                  </button>
                ))}
                {!nearMissPois.length && <div className="mutedLine">None.</div>}
              </div>
            </div>

            {peaksDashErr && <div className="mutedLine">Peaks API error: {peaksDashErr}</div>}
            {peaksDashLoading && <div className="mutedLine">Loading peaks…</div>}
          </div>
        </div>

        <div className="peaksRight">
          <div className="subPanel">
            <div className="subHdrRow">
              <div className="subHdr">CLASS TRACKERS</div>
              <div className="subMeta">{peaksClass.toUpperCase()}</div>
            </div>

            {peaksTrackers.length === 0 && (
              <div className="mutedLine">No class tracker data found for this class.</div>
            )}

            <div className="trackerCards">
              {[primaryTracker, secondaryTracker].filter(Boolean).map((t) => {
                const done = Number(t.done) || 0;
                const total = Number(t.total) || 0;
                const pct = total > 0 ? Math.min(100, Math.round((done / total) * 100)) : 0;
                return (
                  <div key={t.key} className="trackerCard">
                    <div className="trackerTop">
                      <div className="trackerLabel">{String(t.label || t.key).toUpperCase()}</div>
                      <div className="trackerCount">{done} / {total || "—"}</div>
                    </div>
                    <div className="barOuter">
                      <div className="barInner" style={{ width: `${pct}%` }} />
                    </div>
                    <div className="trackerNote">{t.note || trackerMood(pct)}</div>
                  </div>
                );
              })}
            </div>

            <div className="topVisitedHdr">
              <div className="subHdr">TOP VISITED • {peaksClass.toUpperCase()}</div>
              <div className="subMeta">ALL-TIME</div>
            </div>

            <div className="topVisited scrollBox">
              {peaksTopVisited.map((p) => (
                <button
                  type="button"
                  key={p.peak_osm_id || p.name}
                  className="topRow topRowBtn"
                  onClick={() => openDiag(p.peak_osm_id, "peak")}
                  title="Open diagnostics"
                >
                  <div className="topName">{String(p.name || "(unknown)").toUpperCase()}</div>
                  <div className="topSub">last: {p.last || "—"}</div>
                  <div className="topCount">{Number.isFinite(Number(p.count)) ? nf0.format(Number(p.count)) : "—"}</div>
                </button>
              ))}
              {!peaksTopVisited.length && <div className="mutedLine">No peak hits tracked yet.</div>}
            </div>
          </div>
        </div>
      </div>

      {/* Diagnostics drawer */}
      <div className={`diagBackdrop ${diagOpen ? "open" : ""}`} onClick={closeDiag} />
      <aside className={`diagDrawer ${diagOpen ? "open" : ""}`} aria-hidden={!diagOpen}>
        <div className="diagHead">
          <div className="diagTitle">DIAGNOSTICS</div>
          <button className="diagClose" onClick={closeDiag} title="Close">×</button>
        </div>

        {peakItemLoading && <div className="diagBody">Loading…</div>}
        {!peakItemLoading && peakItemErr && <div className="diagBody subtle">Error: {peakItemErr}</div>}

        {!peakItemLoading && peakItem && peakItem.found && (
          <div className="diagBody">
            <div className="diagName">{peakItem.name || "(unknown)"}</div>

            <div className="diagRenameRow">
              <input
                className="diagRenameInput"
                value={renameText}
                onChange={(e) => setRenameText(e.target.value)}
                placeholder="Rename this peak/POI"
              />
              <button className="diagRenameBtn" onClick={onSaveRename} disabled={renameSaving}>
                {renameSaving ? "…" : "SAVE"}
              </button>
            </div>
            {renameMsg && <div className="diagRenameMsg">{renameMsg}</div>}

            <div className="diagMeta">
              <span className="pill">{String(peakItem.kind || (selectedItem?.kind || "poi")).toUpperCase()}</span>
              {peakItem.poi_type && <span className="pill">{String(peakItem.poi_type).toUpperCase()}</span>}
              <span className="pill">OSM {peakItem.peak_osm_id}</span>
              {peakItem.ele_m != null && <span className="pill">{Math.round(Number(peakItem.ele_m))}m</span>}
            </div>

            <div className="diagGrid">
              <div className="diagStat">
                <div className="diagK">bagged</div>
                <div className="diagV">{peakItem.bagged_count ?? 0}</div>
              </div>
              <div className="diagStat">
                <div className="diagK">near</div>
                <div className="diagV">{peakItem.near_count ?? 0}</div>
              </div>
              <div className="diagStat">
                <div className="diagK">last bagged</div>
                <div className="diagV">{peakItem.last_bagged ? String(peakItem.last_bagged).slice(0, 10) : "—"}</div>
              </div>
              <div className="diagStat">
                <div className="diagK">coords</div>
                <div className="diagV">{(peakItem.lat != null && peakItem.lon != null) ? `${Number(peakItem.lat).toFixed(5)}, ${Number(peakItem.lon).toFixed(5)}` : "—"}</div>
              </div>
            </div>

            <div className="diagSub">tags</div>
            <pre className="diagJson">{JSON.stringify(peakItem.tags || {}, null, 2)}</pre>

            {Array.isArray(peakItem.recent_workouts) && peakItem.recent_workouts.length > 0 && (
              <>
                <div className="diagSub">recent workouts</div>
                <div className="diagList">
                  {peakItem.recent_workouts.slice(0, 10).map((w) => (
                    <div key={`${w.workout_id}:${w.started_at}`} className="diagRow">
                      <div className="diagRowA">{String(w.started_at || "").slice(0, 10)}</div>
                      <div className="diagRowB">{String(w.sport || "").toUpperCase()}</div>
                      <div className="diagRowC">{String(w.hit_type || "")}</div>
                    </div>
                  ))}
                </div>
              </>
            )}
          </div>
        )}

        {!peakItemLoading && peakItem && !peakItem.found && (
          <div className="diagBody subtle">Not found.</div>
        )}
      </aside>

      <div className="coachSum">
        <div className="coachSumTitle">COACH SUMMARY</div>
        <div className="coachSumBody">
          Heatmap is Monday-aligned, lists are fixed-height scroll, and diagnostics is a drawer. Next: dedicated Peaks Map.
        </div>
      </div>
    </div>
  );
}
