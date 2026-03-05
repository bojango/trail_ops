import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  ResponsiveContainer,
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
} from "recharts";
import "react-day-picker/style.css";
import "./styles.css";
import { DayPicker } from "react-day-picker";

import { fetchHealth, fetchWorkouts } from "./api";

// --- Units / formatting ------------------------------------------------------

const M_TO_MI = 0.000621371;
const M_TO_FT = 3.28084;

const nf0 = new Intl.NumberFormat("en-GB", { maximumFractionDigits: 0 });
const nf1 = new Intl.NumberFormat("en-GB", {
  maximumFractionDigits: 1,
  minimumFractionDigits: 1,
});

function clampNum(x) {
  const n = Number(x);
  return Number.isFinite(n) ? n : 0;
}

function isoDate(d) {
  return d.toISOString().slice(0, 10);
}

function fmtMonospace(ts) {
  if (!ts) return "";
  return String(ts).slice(0, 16).replace("T", " ");
}

// --- Sparklines (placeholders for per-workout quick glance) ----------------

function statForWorkout(w) {
  const st = String(w?.sport_type || "").toLowerCase();
  const elevFt = clampNum(w?.elevation_gain_m) * M_TO_FT;

  // Running (outdoor): elevation if big vert, otherwise pace.
  if (st === "running:generic") return elevFt > 800 ? "elevation" : "pace";
  if (st === "running:indoor_running") return "heart";

  // Walking / hiking.
  if (st === "walking:generic") return "elevation";
  if (st === "walking:indoor_walking") return "heart";
  if (st.includes("hiking")) return "elevation";

  // Cycling.
  if (st.startsWith("cycling:")) return "power";

  // Strength / HIIT / Stair climber.
  if (st === "training:strength_training") return "heart";
  if (st === "fitness_equipment:stair_climbing") return "heart";
  if (st === "62:70") return "heart";

  // Cooldown / generic etc.
  return "heart";
}

function sparkColor(stat) {
  // Match the requested palette (retro-futuristic but readable).
  if (stat === "heart") return "var(--spark-hr)";
  if (stat === "pace") return "var(--spark-pace)";
  if (stat === "elevation") return "var(--spark-elev)";
  if (stat === "power") return "var(--spark-power)";
  return "var(--spark-pace)";
}

function sparkPoints(seed, n = 18) {
  // Deterministic fake signal based on workout_id.
  const s = clampNum(seed) || 0;
  const pts = [];
  const a = 0.35 + ((s % 13) / 13) * 0.35;
  const f = 0.6 + ((s % 7) / 7) * 1.1;
  const p = (s % 17) * 0.12;
  for (let i = 0; i < n; i++) {
    const t = i / (n - 1);
    const y = 0.55 + a * Math.sin((t * Math.PI * 2) * f + p) + 0.08 * Math.sin(t * 9 + p);
    pts.push(Math.max(0.05, Math.min(0.95, y)));
  }
  return pts;
}

function Sparkline({ stat, seed }) {
  const W = 160;
  const H = 34;
  const P = 3;
  const pts = useMemo(() => sparkPoints(seed), [seed]);
  const d = useMemo(() => {
    if (!pts.length) return "";
    const xStep = (W - P * 2) / (pts.length - 1);
    return pts
      .map((v, i) => {
        const x = P + i * xStep;
        const y = P + (1 - v) * (H - P * 2);
        return `${i === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
      })
      .join(" ");
  }, [pts]);

  const stroke = sparkColor(stat);

  return (
    <div className="rowSpark" title={`${stat} preview (placeholder)`}>
      <svg width={W} height={H} viewBox={`0 0 ${W} ${H}`} role="img" aria-label="sparkline">
        <path className="sparkBg" d={`M ${P} ${H - P} L ${W - P} ${H - P}`} />
        <path className="sparkLine" d={d} style={{ stroke }} />
      </svg>
      <div className="rowSparkLabel">
        {stat === "heart" ? "HR" : stat === "pace" ? "PACE" : stat === "power" ? "PWR" : "ELEV"}
      </div>
    </div>
  );
}

// --- Presets -----------------------------------------------------------------

const RANGE_PRESETS = [
  { key: "last7", label: "Last 7 days", kind: "days", days: 7 },
  { key: "last14", label: "Last 14 days", kind: "days", days: 14 },
  { key: "wtd", label: "Week to date", kind: "wtd" },
  { key: "lastweek", label: "Last week", kind: "lastWeek" },
  { key: "mtd", label: "Month to date", kind: "mtd" },
  { key: "lastmonth", label: "Last month", kind: "lastMonth" },
];

const SPORT_CHIP_OPTIONS = [
  { key: "all", label: "All" },
  { key: "run", label: "Run" },
  { key: "walk", label: "Walk" },
  { key: "hike", label: "Hike" },
  { key: "cycle", label: "Cycle" },
  { key: "strength", label: "Strength" },
  { key: "hiit", label: "HIIT" },
  { key: "stair", label: "Stair climber" },
];


// Peak classification UI stubs (we’ll wire to API later).
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

// Placeholder heatmap: 4 weeks × 7 days (0..3 intensity).
const PEAK_HIT_WEEKS = [
  [0, 0, 1, 0, 2, 0, 0],
  [0, 2, 0, 1, 0, 2, 0],
  [0, 0, 0, 2, 0, 0, 1],
  [1, 0, 2, 0, 0, 1, 0],
];

const CLASS_TRACKERS = [
  { key: "main", label: "Wainwrights", done: 42, total: 214, note: "+6 per quarter at current rate" },
  { key: "alt", label: "Dodds", done: 17, total: 89, note: "Strong momentum" },
];

const TOP_VISITED = [
  { name: "Scafell Pike", last: "2025-09-14", count: 6 },
  { name: "Helvellyn", last: "2025-08-02", count: 5 },
  { name: "Blencathra", last: "2025-10-21", count: 4 },
  { name: "Skiddaw", last: "2026-01-06", count: 4 },
  { name: "Catbells", last: "2025-07-11", count: 3 },
];


// Your exact naming map (client-side for now)
function displaySportType(raw) {
  const s = String(raw || "").toLowerCase();
  const map = {
    "running:generic": "Running (outdoor)",
    "walking:generic": "Walking (outdoor)",
    "62:70": "HIIT",
    "cycling:indoor_cycling": "Cycling (indoor)",
    "fitness_equipment:stair_climbing": "Stair Climber",
    "generic:generic": "Cooldown",
    "running:indoor_running": "Running (indoor)",
    "training:strength_training": "Strength Training",
    "walking:indoor_walking": "Walking (indoor)",
  };
  return map[s] || raw || "Unknown";
}

// NOTE: API currently supports a single sport string or "all".
// For the UX you want (All/Run/Walk/etc), we keep the API call broad
// and filter client-side until we add category support server-side.
function sportMatchesChip(chipKey, rawSportType) {
  if (!chipKey || chipKey === "all") return true;
  const t = String(rawSportType || "").toLowerCase();
  if (chipKey === "run") return t.startsWith("running:");
  if (chipKey === "walk") return t.startsWith("walking:");
  if (chipKey === "hike") return t.startsWith("hiking:");
  if (chipKey === "cycle") return t.startsWith("cycling:");
  if (chipKey === "strength") return t === "training:strength_training";
  if (chipKey === "hiit") return t === "62:70";
  if (chipKey === "stair") return t === "fitness_equipment:stair_climbing";
  return true;
}

function startOfWeek(d) {
  const x = new Date(d);
  const day = (x.getDay() + 6) % 7; // Monday=0
  x.setDate(x.getDate() - day);
  x.setHours(0, 0, 0, 0);
  return x;
}
function startOfMonth(d) {
  const x = new Date(d);
  x.setDate(1);
  x.setHours(0, 0, 0, 0);
  return x;
}
function endOfLastWeek(today) {
  const sow = startOfWeek(today);
  const end = new Date(sow);
  end.setDate(end.getDate() - 1);
  end.setHours(23, 59, 59, 999);
  return end;
}
function startOfLastWeek(today) {
  const e = endOfLastWeek(today);
  return startOfWeek(e);
}
function endOfLastMonth(today) {
  const som = startOfMonth(today);
  const end = new Date(som);
  end.setDate(0);
  end.setHours(23, 59, 59, 999);
  return end;
}
function startOfLastMonth(today) {
  const e = endOfLastMonth(today);
  return startOfMonth(e);
}

// --- Helpers -----------------------------------------------------------------

function groupDaily(items) {
  const map = new Map();
  for (const w of items) {
    const ts = String(w.start_time || "");
    const day = ts.slice(0, 10) || "unknown";
    const cur = map.get(day) || { day, distMi: 0, elevFt: 0, workouts: 0 };
    cur.distMi += clampNum(w.distance_m) * M_TO_MI;
    cur.elevFt += clampNum(w.elevation_gain_m) * M_TO_FT;
    cur.workouts += 1;
    map.set(day, cur);
  }
  return Array.from(map.values()).sort((a, b) => (a.day < b.day ? -1 : 1));
}

function SparkTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  const d = payload[0]?.payload || {};
  return (
    <div className="tip">
      <div className="tipTitle">{label}</div>
      <div className="tipRow">
        <span className="k">Distance</span>
        <span className="v">{nf1.format(d.distMi || 0)} mi</span>
      </div>
      <div className="tipRow">
        <span className="k">Elevation</span>
        <span className="v">{nf0.format(d.elevFt || 0)} ft</span>
      </div>
      <div className="tipRow">
        <span className="k">Workouts</span>
        <span className="v">{nf0.format(d.workouts || 0)}</span>
      </div>
    </div>
  );
}

function useOnClickOutside(ref, handler) {
  useEffect(() => {
    function listener(e) {
      if (!ref.current || ref.current.contains(e.target)) return;
      handler(e);
    }
    document.addEventListener("mousedown", listener);
    document.addEventListener("touchstart", listener);
    return () => {
      document.removeEventListener("mousedown", listener);
      document.removeEventListener("touchstart", listener);
    };
  }, [ref, handler]);
}

// --- App ---------------------------------------------------------------------

export default function App() {
  const today = useMemo(() => new Date(), []);

  // Defaults
  const [preset, setPreset] = useState("last7");
  const [sportChip, setSportChip] = useState("all");
  const [peaksSummaryMode, setPeaksSummaryMode] = useState("ai");
  const [peaksRange, setPeaksRange] = useState("30d");
  const [peaksClass, setPeaksClass] = useState("wainwrights");

  const [start, setStart] = useState(() => {
    const d = new Date(today);
    d.setDate(d.getDate() - 6);
    return isoDate(d);
  });
  const [end, setEnd] = useState(() => isoDate(today));

  // API state
  const [health, setHealth] = useState(null);
  const [healthErr, setHealthErr] = useState(null);
  const [data, setData] = useState(null);
  const [dataErr, setDataErr] = useState(null);

  // Popovers
  const [rangeOpen, setRangeOpen] = useState(false);
  const [sportOpen, setSportOpen] = useState(false);
  const rangeRef = useRef(null);
  const sportRef = useRef(null);
  useOnClickOutside(rangeRef, () => setRangeOpen(false));
  useOnClickOutside(sportRef, () => setSportOpen(false));

  // Custom range picker state
  const [customRange, setCustomRange] = useState(() => ({
    from: new Date(start),
    to: new Date(end),
  }));

  // Apply presets to (start,end)
  useEffect(() => {
    const p = RANGE_PRESETS.find((x) => x.key === preset);
    if (!p) return;

    const t = new Date(today);
    let s;
    let e = new Date(t);

    if (p.kind === "days") {
      s = new Date(t);
      s.setDate(s.getDate() - (p.days - 1));
    } else if (p.kind === "wtd") {
      s = startOfWeek(t);
    } else if (p.kind === "lastWeek") {
      s = startOfLastWeek(t);
      e = endOfLastWeek(t);
    } else if (p.kind === "mtd") {
      s = startOfMonth(t);
    } else if (p.kind === "lastMonth") {
      s = startOfLastMonth(t);
      e = endOfLastMonth(t);
    } else {
      return;
    }

    setStart(isoDate(s));
    setEnd(isoDate(e));
    setCustomRange({ from: s, to: e });
  }, [preset, today]);

  useEffect(() => {
    fetchHealth().then(setHealth).catch(setHealthErr);
  }, []);

  useEffect(() => {
    setDataErr(null);
    // We fetch "all" then filter client-side for now.
    fetchWorkouts({ start, end, sport: "all", limit: 300 })
      .then(setData)
      .catch(setDataErr);
  }, [start, end]);

  const itemsRaw = data?.items || [];
  const items = useMemo(
    () => itemsRaw.filter((w) => sportMatchesChip(sportChip, w.sport_type)),
    [itemsRaw, sportChip]
  );

  const daily = useMemo(() => groupDaily(items), [items]);

  const distMi = items.reduce((a, w) => a + clampNum(w.distance_m) * M_TO_MI, 0);
  const elevFt = items.reduce((a, w) => a + clampNum(w.elevation_gain_m) * M_TO_FT, 0);
  const durHr = items.reduce((a, w) => a + clampNum(w.duration_s) / 3600, 0);
  const mostRecent = items[0] || null;

  const topSportLabel = useMemo(() => {
    const opt = SPORT_CHIP_OPTIONS.find((x) => x.key === sportChip);
    return opt ? opt.label : "All";
  }, [sportChip]);

  function applyCustomRange() {
    const from = customRange?.from;
    const to = customRange?.to;
    if (!from || !to) return;
    setPreset("custom");
    setStart(isoDate(from));
    setEnd(isoDate(to));
    setRangeOpen(false);
  }

  return (
    <div className="mc">
      <header className="mcTop">
        <div className="brand">
          <div className="logo" aria-hidden="true">
            <div className="dot" />
          </div>
          <div className="brandText">
            <div className="brandTitle">TRAILOPS</div>
            <div className="brandSub">Mission console • local-first • private</div>
          </div>
        </div>

        <div className="topFilters">
          {/* RANGE chip */}
          <div className="chipWrap" ref={rangeRef}>
            <button
              className={"chip accent btnChip " + (rangeOpen ? "open" : "")}
              onClick={() => setRangeOpen((v) => !v)}
              aria-expanded={rangeOpen}
              title="Change date range"
            >
              <span className="chipK">RANGE</span>
              <span className="chipV">
                {start} → {end}
              </span>
              <span className="chev" aria-hidden="true">
                ▾
              </span>
            </button>

            {rangeOpen && (
              <div className="popover popRange">
                <div className="popHdr">DATE RANGE</div>

                <div className="popSection">
                  <div className="popLab">Presets</div>
                  <div className="popList">
                    {RANGE_PRESETS.map((p) => (
                      <button
                        key={p.key}
                        className={"popItem " + (preset === p.key ? "active" : "")}
                        onClick={() => {
                          setPreset(p.key);
                          setRangeOpen(false);
                        }}
                      >
                        {p.label}
                      </button>
                    ))}
                  </div>
                </div>

                <div className="popSection">
                  <div className="popLab">Custom</div>
                  <div className="calWrap">
                    <DayPicker
                      mode="range"
                      selected={customRange}
                      onSelect={setCustomRange}
                      numberOfMonths={1}
                      captionLayout="dropdown"
                    />
                    <div className="calFoot">
                      <div className="calVals mono">
                        <span>{customRange?.from ? isoDate(customRange.from) : "start"}</span>
                        <span className="dot">•</span>
                        <span>{customRange?.to ? isoDate(customRange.to) : "end"}</span>
                      </div>
                      <button
                        className="pillBtn"
                        onClick={applyCustomRange}
                        disabled={!customRange?.from || !customRange?.to}
                      >
                        Apply
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>

          {/* SPORT chip */}
          <div className="chipWrap" ref={sportRef}>
            <button
              className={"chip btnChip " + (sportOpen ? "open" : "")}
              onClick={() => setSportOpen((v) => !v)}
              aria-expanded={sportOpen}
              title="Filter by sport"
            >
              <span className="chipK">SPORT</span>
              <span className="chipV">{topSportLabel.toUpperCase()}</span>
              <span className="chev" aria-hidden="true">
                ▾
              </span>
            </button>

            {sportOpen && (
              <div className="popover popSport">
                <div className="popHdr">SPORT</div>
                <div className="popList">
                  {SPORT_CHIP_OPTIONS.map((o) => (
                    <button
                      key={o.key}
                      className={"popItem " + (sportChip === o.key ? "active" : "")}
                      onClick={() => {
                        setSportChip(o.key);
                        setSportOpen(false);
                      }}
                    >
                      {o.label}
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>

          <div className={"chip " + (health ? "ok" : "warn")}
               title={health ? "API reachable" : "API not reachable"}>
            <span className="chipK">API</span>
            <span className="chipV">{health ? "OK" : "DOWN"}</span>
          </div>

          <div className="chip">
            <span className="chipK">MODE</span>
            <span className="chipV">LOCAL</span>
          </div>
        </div>
      </header>

      {/* KPI row (ordered as requested) */}
      <section className="kpiRow">
        <div className="kpi topKpi hoverGlow">
          <div className="kpiK">TIME ON FEET</div>
          <div className="kpiV">
            {nf1.format(durHr)}
            <span className="kpiUnit"> hr</span>
          </div>
          <div className="kpiHint">range</div>
          <div className="kpiHelp">Total duration across the selected range.</div>
        </div>

        <div className="kpi topKpi hoverGlow">
          <div className="kpiK">DISTANCE</div>
          <div className="kpiV">
            {nf1.format(distMi)}
            <span className="kpiUnit"> mi</span>
          </div>
          <div className="kpiHint">range</div>
          <div className="kpiHelp">Total distance across the selected range.</div>
        </div>

        <div className="kpi topKpi hoverGlow">
          <div className="kpiK">ELEVATION GAIN</div>
          <div className="kpiV">
            {nf0.format(elevFt)}
            <span className="kpiUnit"> ft</span>
          </div>
          <div className="kpiHint">range</div>
          <div className="kpiHelp">Total elevation gain across the selected range.</div>
        </div>

        <div className="kpi topKpi hoverGlow">
          <div className="kpiK">VO2 MAX</div>
          <div className="kpiV">—</div>
          <div className="kpiHint muted">placeholder</div>
          <div className="kpiHelp">Estimated aerobic fitness. We’ll wire this later.</div>
        </div>

        <div className="kpi topKpi hoverGlow">
          <div className="kpiK">RESTING HEART RATE</div>
          <div className="kpiV">—</div>
          <div className="kpiHint muted">placeholder</div>
          <div className="kpiHelp">Resting HR trend. Needs an endpoint later.</div>
        </div>

        <div className="kpi topKpi hoverGlow">
          <div className="kpiK">WORKOUTS</div>
          <div className="kpiV">{nf0.format(items.length)}</div>
          <div className="kpiHint okDot">loaded</div>
          <div className="kpiHelp">Count of workouts in the selected range.</div>
        </div>
      </section>

      <section className="alerts">
        <div className="alertsLeft">
          <span className="badge">ALERTS & ANOMALIES</span>
          <span className="alertsHint">Stub. This becomes interactive later.</span>
        </div>
        <div className="alertsRight">
          <span className="pill warn">LOAD elevated</span>
          <span className="pill muted">HR DRIFT on climbs</span>
          <span className="pill ok">RHR stable</span>
        </div>
      </section>

      <section className="mainGrid">
        <div className="panel span2">
          <div className="panelHdr">
            <div className="panelTitle">PRIMARY TELEMETRY</div>
            <div className="panelSub">
              Distance by day (line) + elevation by day (bars). More modes later.
            </div>
          </div>

          <div className="chartWrap">
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={daily}>
                <CartesianGrid stroke="rgba(255,255,255,0.06)" vertical={false} />
                <XAxis
                  dataKey="day"
                  tick={{ fill: "rgba(255,255,255,0.55)", fontSize: 12 }}
                />
                <YAxis yAxisId="left" tick={{ fill: "rgba(255,255,255,0.55)", fontSize: 12 }} />
                <YAxis
                  yAxisId="right"
                  orientation="right"
                  tick={{ fill: "rgba(255,255,255,0.55)", fontSize: 12 }}
                />
                <Tooltip content={<SparkTooltip />} />
                <Bar yAxisId="right" dataKey="elevFt" fill="rgba(255,138,31,0.35)" />
                <Line
                  yAxisId="left"
                  type="monotone"
                  dataKey="distMi"
                  stroke="rgba(255,255,255,0.78)"
                  strokeWidth={2}
                  dot={false}
                />
              </ComposedChart>
            </ResponsiveContainer>
          </div>

          <div className="panelFoot">
            {dataErr ? (
              <span className="err">Failed to load workouts: {String(dataErr.message || dataErr)}</span>
            ) : (
              <span className="mutedLine">
                Showing {nf0.format(items.length)} workouts • {nf0.format(daily.length)} days • Sport: {topSportLabel.toLowerCase()}
              </span>
            )}
            {healthErr && (
              <span className="err">API health check failed: {String(healthErr.message || healthErr)}</span>
            )}
          </div>
        </div>

        <div className="panel">
          <div className="panelHdr">
            <div className="panelTitle">RECENT ACTIVITY</div>
            <div className="panelSub">Most recent workout in current range</div>
          </div>

          {mostRecent ? (
            <div className="recent">
              <div className="recentTop">
                <div className="recentName">{displaySportType(mostRecent.sport_type)}</div>
                <div className="recentTag mono">#{mostRecent.id}</div>
              </div>
              <div className="recentMeta mono">{fmtMonospace(mostRecent.start_time)}</div>

              <div className="miniStats">
                <div className="mini">
                  <div className="k">Distance</div>
                  <div className="v">{nf1.format(clampNum(mostRecent.distance_m) * M_TO_MI)} mi</div>
                </div>
                <div className="mini">
                  <div className="k">Time</div>
                  <div className="v">{nf0.format(Math.round(clampNum(mostRecent.duration_s) / 60))} min</div>
                </div>
                <div className="mini">
                  <div className="k">Elevation</div>
                  <div className="v">{nf0.format(Math.round(clampNum(mostRecent.elevation_gain_m) * M_TO_FT))} ft</div>
                </div>
              </div>

              {/* Map placeholder: we wire real route data in Phase 2B */}
              <div className="mapStub">
                <div className="mapStubHdr">ROUTE MAP</div>
                <div className="mapStubBody">Map comes next (Phase 2B). No need to “rebuild” logic, just a new renderer.</div>
              </div>

              <div className="recentBtns">
                <button className="btn" disabled>
                  OPEN ACTIVITY
                </button>
                <button className="btn ghost" disabled>
                  EXPLAIN
                </button>
              </div>

              <div className="coachSummary">
                <div className="coachLabel">COACH SUMMARY</div>
                <div className="coachText">Placeholder. This will be filled by the coach later.</div>
              </div>
            </div>
          ) : (
            <div className="empty">No workouts in range.</div>
          )}
        </div>

        <div className="panel coach">
          <div className="panelHdr">
            <div className="panelTitle">AI COACH</div>
            <div className="panelSub">Placeholder shell. We wire chat + actions later.</div>
          </div>

          <div className="coachBody">
            <div className="coachMsg sys">
              <div className="mono">System:</div>
              <div className="mutedLine">Coach will drive filters and query panels through the API.</div>
            </div>

            <div className="coachMsg user">
              <div className="mono">You:</div>
              <div className="mutedLine">“Show last 14 days and focus on distance.”</div>
            </div>

            <div className="coachMsg coach">
              <div className="mono">Coach:</div>
              <div className="mutedLine">
                Switching range preset to <b>Last 14 days</b>.
              </div>
              <button className="pillBtn" onClick={() => setPreset("last14")}>
                Apply
              </button>
            </div>
          </div>

          <div className="coachInput">
            <input
              placeholder="Ask: 'show GAP monthly' • 'surface last 12' • 'HR drift on climbs'"
              disabled
            />
            <button className="send" disabled>
              SEND
            </button>
          </div>
        </div>
        <section className="lowerGrid">
  <div className="panel span2 peaksPanel">
    <div className="panelHdr panelHdrRow">
      <div>
        <div className="panelTitle">PEAKS • LISTS • PROGRESS</div>
        <div className="panelSub">Stub UI. Real peak maths lands when the API exposes peak hits, unique peaks, and class totals.</div>
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
        <div className="miniV">9</div>
        <div className="miniS">12w</div>
      </div>
      <div className="miniCard">
        <div className="miniK">UNIQUE PEAKS</div>
        <div className="miniV">7</div>
        <div className="miniS">12w • 2 repeats</div>
      </div>
      <div className="miniCard">
        <div className="miniK">LIFETIME UNIQUE</div>
        <div className="miniV">64</div>
        <div className="miniS">tracked</div>
      </div>
      <div className="miniCard">
        <div className="miniK">PEAK RATE</div>
        <div className="miniV">0.58</div>
        <div className="miniS">/wk • rolling</div>
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
            <div className="lowHigh">LOW → HIGH</div>
          </div>

          <div className="hitGrid">
            {PEAK_HIT_WEEKS.map((week, wi) => (
              <div key={wi} className="hitRow">
                {week.map((v, di) => (
                  <div key={di} className={`hitCell hit${v}`} title={`${v} hits`} />
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
      </div>

      <div className="peaksRight">
        <div className="subPanel">
          <div className="subHdrRow">
            <div className="subHdr">CLASS TRACKERS</div>
            <div className="subMeta">{peaksClass.toUpperCase()}</div>
          </div>

          {CLASS_TRACKERS.map((t) => (
            <div key={t.key} className="tracker">
              <div className="trackerTop">
                <div className="trackerName">{t.label.toUpperCase()}</div>
                <div className="trackerCount">{t.done} / {t.total}</div>
              </div>
              <div className="barOuter">
                <div className="barInner" style={{ width: `${Math.min(100, Math.round((t.done / t.total) * 100))}%` }} />
              </div>
              <div className="trackerSub">{t.note}</div>
            </div>
          ))}

          <div className="topVisitedHdr">
            <div className="subHdr">TOP VISITED • {peaksClass.toUpperCase()}</div>
            <div className="subMeta">ALL-TIME</div>
          </div>

          <div className="topVisited">
            {TOP_VISITED.map((p) => (
              <div key={p.name} className="topRow">
                <div className="topName">{p.name.toUpperCase()}</div>
                <div className="topSub">last: {p.last}</div>
                <div className="topCount">{p.count}</div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>

    <div className="coachSum">
      <div className="coachSumTitle">COACH SUMMARY</div>
      <div className="coachSumBody">
        When peak data is wired, coach will point out what’s trending (new peaks, repeats, and progress pacing vs target).
      </div>
    </div>
  </div>

  <div className="panel span2">
    <div className="panelHdr">
      <div className="panelTitle">SNAPSHOT</div>
      <div className="panelSub">4 quick charts. Real metrics land as API expands.</div>
    </div>

    <div className="snapGrid">
      <div className="snap">
        <div className="snapK">PACE VS GAP</div>
        <div className="snapV">Placeholder</div>
      </div>
      <div className="snap">
        <div className="snapK">HR ZONES</div>
        <div className="snapV">Placeholder</div>
      </div>
      <div className="snap">
        <div className="snapK">TERRAIN MIX</div>
        <div className="snapV">Placeholder</div>
      </div>
      <div className="snap">
        <div className="snapK">PEAK HITS</div>
        <div className="snapV">Placeholder</div>
      </div>
    </div>

    <div className="coachSum">
      <div className="coachSumTitle">COACH SUMMARY</div>
      <div className="coachSumBody">Coach will summarise what these charts mean once the AI layer is plugged in.</div>
    </div>
  </div>
</section>



        <div className="panel span2">
          <div className="panelHdr">
            <div className="panelTitle">RECENT WORKOUTS</div>
            <div className="panelSub">Fixed height + scroll. Activities page later.</div>
          </div>

          <div className="list scrollList">
            {items.slice(0, 50).map((w) => (
              <div key={w.id} className="row">
                <div className="rowMain">
                  <div className="rowTop">
                    <span className="mono">{fmtMonospace(w.start_time)}</span>
                    <span className="pill">{displaySportType(w.sport_type)}</span>
                  </div>
                  <div className="rowSub">
                    <span>{nf1.format(clampNum(w.distance_m) * M_TO_MI)} mi</span>
                    <span className="dot">•</span>
                    <span>{nf0.format(Math.round(clampNum(w.duration_s) / 60))} min</span>
                    <span className="dot">•</span>
                    <span>{nf0.format(Math.round(clampNum(w.elevation_gain_m) * M_TO_FT))} ft</span>
                  </div>
                </div>

                {/* Quick-glance mini chart per workout (placeholder; API wiring later). */}
                <Sparkline stat={statForWorkout(w)} seed={w.id} />

                <div className="rowId mono">#{w.id}</div>
              </div>
            ))}
            {!data && !dataErr && <div className="mutedLine">Loading…</div>}
          </div>
        </div>
      </section>
    </div>
  );
}
