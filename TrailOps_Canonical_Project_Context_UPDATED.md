# TrailOps – Canonical Project Context (Authoritative)

## Project Overview
TrailOps is a local-first, personal training analytics system built in Python.  
It ingests workout data from HealthFit exports (FIT files), computes derived metrics offline, stores results in SQLite, and presents insights via a Streamlit dashboard. The fininshed dashboard will include an AI coach integration using a local AI LLM model that's sole aim is to become an expert on my health and workout data to be able to provide insights, analysis and suggestions on training. 

TrailOps exists to provide accurate, transparent, long-term training insight without reliance on cloud platforms or opaque algorithms.  
It is designed for a **single user**, focused on correctness and reproducibility rather than social features or gamification. 

Core philosophy:
- Local-first and privacy-focused
- Offline by default
- Transparent metrics over black-box scoring
- Correctness over cleverness

---

## Project Aims
- Build a trustworthy personal training history spanning years
- Derive meaningful metrics that can be explained and audited with key stats
- Enable comparison across runs, terrains, and time
- Avoid vendor lock-in (Strava, Garmin, etc.)
- Serve as a foundation for future analysis and experimentation
- Create an AI coach that is an expert on my training and health data and can provide actional insights
- Display everything on a beautifully designed dashboard that has a retro-futuristic/cassette futuristic aesthetic
- Final dashboard will be hosted locally but available online and mobile friendly

---

## Developer Context (Important)
- I am a complete beginner at coding
- I rely on ChatGPT to write and modify code
- Optimise for correctness, clarity, and maintainability over minimal diffs
- Prefer simple, explicit implementations over clever abstractions

---

## Working Rules for ChatGPT
- Assume I will copy and run your code directly
- Prefer **full-file replacements** delivered as downloadable files
- Avoid partial snippets unless explicitly asked
- Do not overexplain
- Keep explanations short and focused on:
  - What changed
  - Why it changed
- Always think long-term: no quick fixes that cause future breakage
- Ask before refactoring or changing architecture
- Surface assumptions before writing code when relevant
- At the start of each chat, always ask for the files you need to be uploaded so you can see exactly how the code is written
- Always take time to fully understand the code, never give updated files with indentation errors or 'code grammar' errors that will break the dashboard

---

## Non-Negotiable Technical Constraints
- Python
- Local-only execution
- SQLite for persistence
- Streamlit for UI
- Offline-first operation
- No subscriptions or external APIs unless explicitly approved

---

## High-Level Architecture
- **Ingestion layer**: Reads and normalises HealthFit FIT exports
- **Analysis / derived metrics layer**: Computes metrics offline (GAP, moving time, etc.)
- **Storage layer**: Persists raw and derived data in SQLite
- **UI / dashboard layer**: Read-only Streamlit app consuming precomputed data

---

## Directory Structure
trail_ops/
├── app/
│ ├── analysis/ # Derived metric computation (plot_samples.py, gap logic)
│ ├── tools/ # Backfill and maintenance scripts
│ ├── training_dashboard.py # Streamlit UI
│ └── ingestion/ # FIT ingestion logic
├── data/
│ └── trailops.db # SQLite database (single source of persisted truth)
├── .venv/ # Local virtual environment
├── TrailOps_Start_All.bat # Convenience launcher
└── trailops_tree.txt # Directory tree reference


---

## Completed Phases

### Ingestion
**Status:** Complete  
- FIT files ingested reliably
- Robust to missing / dirty data
- Supports running, walking, hiking, cycling, HIIT, cooldowns
- All workouts persisted to SQLite

### Derived Metrics
**Status:** Complete  
**STABLE – v1 (do not modify unless explicitly requested)**

- Moving time
- Moving pace
- Total time
- Total pace
- Stationary time
- Grade Adjusted Pace (GAP)

All metrics are computed **offline**, stored in the database, and treated as immutable once backfilled.

---

## Definitions (Authoritative)

### Total Time
The full elapsed duration of the workout from start to finish, regardless of movement.

### Moving Time
Time spent actively moving, determined by:
- Distance-derived speed
- Distance progression thresholds
- Short, conditional debounce to smooth brief pauses  
Designed to closely match Strava while remaining transparent.

### Stationary Time
`Total Time – Moving Time`

### Total Pace
Overall pace calculated using:
Total Time / Total Distance

### Moving Pace
Pace calculated using:
Moving Time / Distance Covered While Moving

### GAP (Grade Adjusted Pace)
An estimate of the equivalent flat-ground pace for a run, based on:
- Distance-domain smoothed grade
- Minetti-based energy cost model
- Downhill and uphill sanity caps

GAP is computed as a **single average value** per workout.  
No GAP time-series charts are used.

---

## Known Limitations
- Moving time may differ from Strava by ~1–2% on stop-heavy trail runs
- No use of proprietary device pause/resume hints
- GPS noise handling is heuristic-based, not sensor-fused
- GAP accuracy depends on elevation data quality

These limitations are explicitly accepted.

---

## Design Decisions (Why Things Are the Way They Are)
- Derived metrics are computed offline to keep the UI fast and deterministic
- SQLite chosen for simplicity, portability, and transparency
- Streamlit UI is read-only by design
- Metrics prioritise explainability over exact vendor replication
- v1 metrics are considered stable once validated against real data

---

## Out of Scope (Explicit)
TrailOps will not:
- Replicate Strava’s social features
- Provide coaching plans or prescriptive training advice
- Sync data to the cloud
- Require accounts, subscriptions, or logins
- Chase exact vendor parity at the expense of clarity

---

## Map phase


### Map phase (v1 – Stable baseline) — 2026-02-06

**Status:** Working, no known errors. This is the rollback baseline for future map work.

**What’s working (confirmed):**
- Charts render correctly (time‑series: pace, elevation, grade, heart rate, cadence, power).
- Route map renders reliably using **Folium + Leaflet + OpenStreetMap tiles** (no API keys).
- Routes draw correctly for multiple workouts.
- Robust coordinate handling:
  - Detects/handles FIT GPS semicircles and converts to degrees when needed.
  - Handles swapped lat/lon edge cases.
  - Drops impossible coordinates safely.
- Auto zoom to route via `fit_bounds()`.

**Key files involved:**
- `app/training_dashboard.py` (map rendering + UI)
- `app/analysis/map_points.py` (map-point retrieval + helpers)
- `app/tools/backfill_map_points.py` (generates/backfills `workout_map_points`)
- `app/db/database.py` (schema includes `workout_map_points` + markers table)

**Known-good baseline file:**
- `training_dashboard_MAP_ROUTEFIX.py` (the version where charts + map + route are confirmed working)

**Notes:**
- Detail “levels” currently do not show visible differences in the UI (default level works).
- Custom marker UI exists but is not required for baseline stability.

### Map phase (v1.1 – Minor UI refinements) — Planned next

These are intentionally *small* changes to keep stability before bigger map upgrades:

1) **Remove detail level selector**  
   - Always use the default/first available map level internally.

2) **Make map square**  
   - Render the map in a square container so routes fill the view more naturally.

3) **Add route style selector (plumbing only)**  
   - UI selector: `Normal / Elevation / Pace`
   - For v1.1, only **Normal** is implemented (solid orange line).  
   - Elevation/Pace colouring comes in the next larger phase.

### Map phase (v2 – Feature upgrades) — Planned (next chat)

- **Metric-coloured route line:** elevation gradient and pace gradient (Workoutdoors-style).
- **Mile/KM markers:** numbered markers every mile/km (toggle).
- **Zoom-to-route button:** explicit control to recenter/fit bounds.
- **Map ↔ chart sync:** hover (desktop) and touch/hold (mobile) to show:
  - current point on route + stats (pace, elevation, distance, time)
- **Region detection:** classify workouts by area (e.g., Lake District) for analysis + AI coach.
- **Surface type estimation:** road vs trail using OSM tags (future AI coach queries).
- **Heatmap overlay:** later a dedicated heatmap view of all routes (separate section/page).

---

## Map phase (v2 – Progress Update: Phase 2.0–2.2) — 2026-02-08

**Status:** Complete and stable. This is the new rollback point for map work.

### Phase 2.0 – Metric-coloured routes (Completed)
- Implemented **route colouring by metric** with selectable modes:
  - **Normal:** solid orange (unchanged baseline)
  - **Elevation:** coloured by **grade (rise/run)**, not raw elevation
  - **Pace:** coloured by **pace (min/mi)**
- Elevation colour scheme:
  - Downhill → blue
  - Flat → purple
  - Uphill → red
  - Smooth gradient transitions (no blocky segments, no white midpoint)
- Increased elevation gradient contrast by brightening uphill red and downhill blue.
- Pace colour scheme:
  - Faster → green
  - Slower → red
  - Robust percentile scaling to avoid outliers dominating the colour range.
- Applied **light smoothing** (rolling median) to grade and pace to reduce GPS jitter artefacts.

### Stability & performance fixes
- Removed NumPy `Mean of empty slice` warnings by replacing `np.nanmean([a, b])` with a safe two-value averaging helper.
- Reduced unnecessary Streamlit reruns when interacting with the map by:
  - Rendering Folium with a stable `key`
  - Disabling map interaction feedback using `returned_objects=[]` (with fallback for older `streamlit-folium` versions).
- Rebased all changes onto a known-working dashboard version to avoid regressions.
- All delivered files were syntax-checked (AST parsed) before use.

### Phase 2.1 – Distance markers (Completed)
- Added **distance markers along the route**:
  - Toggle: Off / Miles / KM
  - Default: Miles
- Markers are subtle and snapped to the nearest route point using `distance_m`.
- Marker design:
  - Dot is the true anchor on the route
  - Label (`1mi`, `2mi` / `1km`, `2km`) is offset to the right
- Marker anchoring implemented using:
  - `folium.DivIcon(icon_size=(8,8), icon_anchor=(4,4))`
- Marker count is capped to prevent visual clutter on long routes.
- Removed pandas `FutureWarning` by replacing deprecated `fillna(method="ffill")` with `ffill()`.

### Phase 2.2 – Zoom-to-route button (Completed)
- Added a **persistent zoom-to-route control button** (⌂) to the map.
- Implemented as a Leaflet control injected via **Branca `MacroElement` + `Template`**.
- Clicking the button recentres the map to the full route bounds with padding.
- Removed Streamlit toggle; the button is always visible for simplicity and robustness.

---

## Map phase (v2 – Phase 2.3) - Completed (Desktop-first)

### Elevation Profile Hover Cursor (Workoutdoors-style)

**Status:** Implemented, stable, and accepted as the Phase 2.3 outcome. This is the current rollback point for chart–map interaction work.

### Objective
Provide a **Workoutdoors-style elevation profile interaction** that allows smooth inspection of workout data at any point along the route, prioritising responsiveness, clarity, and zero UI lag within Streamlit constraints.

The original plan to synchronise a live map dot with the cursor was intentionally **de-scoped** after evaluation, as the elevation profile alone delivers most of the analytical value with far less complexity and technical debt.

### Final Interaction Model
- Static elevation profile rendered beneath the map
- Native Plotly hover interaction (no Streamlit reruns)
- Vertical spike line follows cursor smoothly
- No zoom or pan on the profile
- No click-to-pin or locking behaviour

### Stats Shown at Cursor
- Distance (mi / km)
- Elevation (ft / m)
- Time (HH:MM:SS)
- Pace (or `N/A`)
- Heart Rate (or `N/A`)
- Power (or `N/A`)

Data is sourced from `workout_map_points` and `workout_plot_samples`, matched by nearest timestamp. Missing data safely degrades to `N/A`.

### UX & Visual Refinements
- Removed unified-hover header values
- Explicit units added to all tooltip values
- Legend / trace labels removed
- Minimal, purpose-built profile styling

### Map Dot Synchronisation
- Explicitly deferred to avoid rerender lag and additional dependencies

### Mobile Behaviour (Current)
- Tap shows tooltip at nearest point
- Repeated taps move the tooltip
- Drag-to-scrub behaviour varies by browser
- No mobile-specific slider fallback yet

### Known Warnings / Maintenance (Next Update)
- Streamlit deprecation warning present:
  - `use_container_width=True` must be replaced with `width="stretch"` or `width="content"`
- This is a non-functional cleanup task to be handled next

### Net Result
- Smooth, zero-lag elevation profile inspection
- Rich contextual stats without UI churn
- No new dependencies
- Stable foundation for future map or mobile enhancements

###Route Context, Terrain, Peaks & Weather Phase — Current State (2026-02-10)
Purpose of this Phase

Extend TrailOps beyond raw metrics by enriching GPS-based workouts (running, walking, hiking) with contextual intelligence so that future analysis and the AI coach can reason about:

Where activities take place (granular + hierarchical locations)

What terrain they are performed on

Which summits / peaks are visited

Environmental conditions (weather, temperature, wind, precipitation)

Long-term patterns such as:

“Where do I run most?”

“What terrain do I mostly train on?”

“Which peaks do I revisit most?”

“How often do I run in rain / heat / wind?”

All enrichment is computed offline, stored in SQLite, and kept out of the UI to preserve dashboard performance.

What Has Been Implemented
1. Route Context (Location Intelligence)

Status: Mostly working, schema stabilisation ongoing

For GPS workouts only:

Uses start point as the primary location anchor

Reverse-geocodes using OpenStreetMap / Nominatim

Stores multiple hierarchical layers:

location_label (closest village / hamlet e.g. Heysham Village)

locality

district

county

region

country

country_code

Raw geocoder response stored as JSON for future AI use

Provider + version tracked

Design decision

Dashboard displays smallest meaningful settlement

AI coach has access to full hierarchy

Enables questions like:

“How many runs were in Lancashire?”

“How many activities in the Lake District?”

2. Terrain / Surface Classification

Status: Working but data quality being refined

Uses Overpass API (OSM) against route points

Classifies surfaces into a two-layer model:

Generalised layer (used for stats + UI):

road

paved_path

trail

track

grass

rock

forest

unknown

Detailed layer (stored as JSON for AI coach):

full OSM surface / highway / tracktype tags

Distance in meters computed per surface type

Unknown surface still present on many routes due to:

OSM data gaps

Sparse tagging on minor paths

3. Peak / Summit Detection

Status: Working, extended and validated

Queries OSM for peaks within 50m of route

Stores:

peak_id (OSM / Wikidata)

name

lat / lon

elevation (meters)

wikidata / wikipedia links when available

Tracks per-workout peak hits

Supports future:

First ascent vs repeat visits

Visit counts per peak

“Most visited summit”

Planned extension:

Classification sets (Wainwrights, Munros, etc.)

4. Weather Context (Added but not fully validated)

Status: Schema + hooks added, validation pending

Designed to store:

Ground-level weather at start point

Summit-level weather (for mountain activities)

Fields include:

temperature

wind

precipitation

weather code

lunar phase + illumination (added now)

Intended to support:

“How often do I run in rain?”

“Hot-weather vs cold-weather performance”

Mountain vs valley weather comparison

How This Is Implemented Technically
New / Extended Tables

workout_route_context

workout_surface_stats

peaks

workout_peak_hits

workout_weather

Backfill Strategy

Backfills are run via CLI tools, not the UI

GPS-only filtering supported

Rate-limited to avoid Overpass timeouts

Initial test backfills confirmed working

Full GPS dataset ≈ 1144 workouts

Current Problems / Blockers (Critical)

This phase exposed schema drift caused by multiple iterations.

Root Cause

SQLite schema evolved incrementally

Columns were added manually or in older code versions

Newer code assumes columns that don’t exist

SQLite enforces constraints strictly

Current Failure Modes

Missing columns

e.g. location_label, rock_m, computed_at

NOT NULL constraint failures

Columns exist but inserts don’t populate them

SQL syntax errors

JSON accidentally interpolated into SQL

Table mismatch

Code querying tables (workouts) from the wrong DB file

Key Insight

This is not random bugs.
It is a migration problem, not a logic problem.

What Must Happen Next (High Priority)

Freeze the schema

Define a single authoritative DB schema

No more ad-hoc column additions

Make the code schema-tolerant

Detect existing columns at runtime

Only insert fields that exist

Provide defaults for NOT NULL fields

One-time stabilisation migration

Add missing columns

Relax NOT NULL constraints where appropriate

Backfill safe defaults

Then resume full backfill

GPS-only

Location → Surface → Peaks → Weather

No UI changes until data is stable

Current State Summary

✔ Core logic for location, terrain, peaks, weather exists

✔ Small backfills work once schema aligns

❌ Full backfill blocked by schema drift

❌ Need a final migration + stabilisation pass

🚫 No more feature expansion until schema is locked

This is the handover point.

Route Context, Terrain, Peaks & Weather Phase — Stabilised (2026-02-16)
Purpose of This Phase

Enrich GPS-based workouts with contextual intelligence so future analytics and the AI coach can reason about:

Where activities take place

What terrain they are performed on

Which summits / landmarks are visited

Environmental conditions

This data is:

Computed offline

Stored in SQLite

Never computed in the UI

Designed for long-term, explainable analysis

What Was Implemented & Stabilised
1️⃣ GPS Filtering (Critical Stability Fix)

Backfills now:

Only target workouts that actually have GPS map points

Ignore indoor / HIIT / treadmill workouts

Use workout_map_points presence as authoritative GPS indicator

Result:

No more no_route_points errors during full passes

Backfill workload = 1144 GPS workouts only

2️⃣ Peak Detection – Full Rewrite (Major Upgrade)

Peak detection logic was completely redesigned.

Old Behaviour

Queried Overpass around individual route points

50m radius

Missed peaks between sampled points

Under-counted major ridge traverses

New Behaviour (Stable)

Compute full route bounding box

Expand with padding (configurable)

Single Overpass query for all POIs within that box

Locally compute distance to downsampled route points

Classify hits based on actual minimum distance

This ensures:

Ridge traverses are correctly captured

Multi-peak mountain days are fully represented

Overpass calls reduced to 1 per workout (instead of many)

POI Expansion

Instead of only natural=peak, the system now pulls:

natural=peak

natural=saddle

natural=ridge

natural=volcano

natural=hill

tourism=viewpoint

man_made=cairn

mountain_pass=yes

All POIs are stored.
Classification can happen later.

Result:

Richer route context

More interesting data

Filtering becomes a UI concern, not a data concern

3️⃣ Duplicate / OSM ID Normalisation

Previous issue:

Mixed storage of peak IDs (INTEGER vs osm_node:xxxx)

Caused NOT NULL constraint failures

Caused duplicate entries

Now:

All peak IDs stored consistently as text

peak_osm_id always populated

Insert logic defensive and schema-aware

Database integrity now stable.

4️⃣ Overpass Stability Improvements

Previously:

Frequent 429 (Too Many Requests)

504 timeouts

Large batch runs failing mid-way

Now implemented:

Minimum delay between Overpass calls

Retry logic for 429 / 504

Backfill runs single Overpass query per workout

Reduced API surface area dramatically

Result:

Stable long backfills

Fewer cancellations

More predictable execution time

5️⃣ Surface Classification Stabilisation

Issue:
NOT NULL constraint failed: workout_surface_stats.computed_at

Root cause:
Insert statements not always populating computed_at.

Fix:

All inserts now explicitly set:

provider

surface_version

computed_at

No implicit assumptions about schema defaults

Surface backfill now stable.

6️⃣ Schema Drift Handling

Major lesson from this phase:

This was not a logic problem.
It was a schema drift problem.

Key decisions:

No more ad-hoc column additions

All inserts must explicitly populate required fields

Code must assume SQLite enforces NOT NULL strictly

Schema must be treated as authoritative

From now on:
Schema changes require deliberate migration logic.

Current Status (Post-Stabilisation)

✔ GPS-only filtering works
✔ Bounding box peak detection implemented
✔ Expanded POI support implemented
✔ Overpass retry logic implemented
✔ Surface NOT NULL issues resolved
✔ Sample high-peak workouts validated manually
✔ Duplicate peak entries cleaned

Full GPS dataset: 1144 workouts
Backfill now completes successfully.

This is the new stable baseline for contextual enrichment.

Next Step (New Chat Objective)

Before modifying ingestion:

We must validate that data quality is high across:

workout_route_context

workout_surface_stats

workout_peak_hits

peaks

workout_weather

This includes:

Checking column completeness

Checking JSON payload sizes

Checking per-workout surface totals

Checking peak density distribution

Checking weather coverage

Checking for NULL drift

Checking for extreme outliers

No ingestion changes until enrichment data is verified stable.

Best Way of Working With Downloaded Files (Authoritative Process)

You download files to:

C:\Users\calum\Downloads

Correct replacement process:

Activate venv

Clear cache

Copy from Downloads → destination

Verify function exists

Reload module

Confirm expected symbols exist

Only then run scripts

PowerShell template:

cd C:\trail_ops
.venv\Scripts\Activate.ps1

Remove-Item "C:\trail_ops\app\analysis\__pycache__" -Recurse -Force -ErrorAction SilentlyContinue

Copy-Item "C:\Users\calum\Downloads\route_context_NEW.py" `
          "C:\trail_ops\app\analysis\route_context.py" -Force

Select-String -Path "C:\trail_ops\app\analysis\route_context.py" `
              -Pattern "compute_and_store_peak_hits" -SimpleMatch

python -c "import importlib, app.analysis.route_context as rc; importlib.reload(rc); print(rc.__file__)"


Never:

Partially paste code

Leave both NEW and old versions lying around

Assume Python is loading what you think it is

Always verify the import target.

Training Dashboard & Schema Stabilisation Phase — 2026-02-19
Purpose of This Phase

After completing the large enrichment backfill (location, terrain, peaks, weather), the goal was to:

Validate that all 1144 GPS workouts were enriched correctly

Ensure schema stability after multiple iterations

Restore full functionality of the training_dashboard.py

Prepare ingestion to compute enrichment for new workouts

This phase focused on stability, validation, and schema alignment, not feature expansion.

What Was Completed
1️⃣ Full GPS Enrichment Backfill

Backfill executed across 1144 GPS workouts including:

Route context (hierarchical location)

Surface stats (v2)

Peak detection (validated bucket distribution)

Weather (start + high point)

Validation checks confirmed:

2288 weather rows (2 per workout)

1144 workouts with weather coverage

Surface stats present for all workouts

Peak bucket distribution realistic

Weather null rates = 0 for new fields (cloud, humidity, wind direction, dewpoint)

Weather enrichment is considered stable.

Peaks enrichment is considered stable.

Surface enrichment remains imperfect (high unknown%), but accepted for now to unblock progress.

2️⃣ Schema Drift Identified

During validation and dashboard restoration, several issues surfaced:

Missing columns between versions

NOT NULL constraint conflicts

Insert mismatches due to evolving schema

Table shape inconsistencies

Column order dependency in dashboard queries

Key insight:

This is a migration problem, not a logic problem.

SQLite’s loose typing combined with incremental column additions caused inconsistent runtime behaviour.

3️⃣ Training Dashboard Failure Cascade

Once enrichment stabilised, the training dashboard failed due to multiple cascading issues:

A) Import Errors

get_geo_areas missing

get_db_file missing

Import-time execution errors (wp_start undefined)

Fixed via:

Route context compatibility wrapper

Guarding import-time logic

B) Aggregate Alias Bug

training_summary.py was returning column labels like:

"SUM(distance_m)"


instead of numeric values.

Fix:

Added SQL aliases (AS dist_m)

Explicit fetchall() handling

Defensive float casting

C) Key Mismatch

Dashboard expected:

summary["lifetime"]


Summary file returned:

summary["all_time"]


Fix:

Return structure aligned with dashboard expectations.

D) Numeric Type Failures

Critical issue:

TypeError: unsupported operand type(s) for /: 'str' and 'float'


Root cause:
SQLite returning numeric columns as TEXT → Pandas receiving object dtype.

Dashboard performed arithmetic without coercion:

df["distance_m"] / 1609.344


This will continue to happen anywhere arithmetic is performed without explicit numeric coercion.

This is a systemic type normalisation issue.

E) Datetime Comparison Failure
Invalid comparison between dtype=datetime64[ns] and date


Cause:
Mixing .dt.date comparisons with Python date objects.

Resolved via Timestamp comparison approach.

Current State

✔ Enrichment data present and validated
✔ Ingestion dashboard works
❌ Training dashboard unstable due to Pandas dtype inconsistencies
❌ Multiple areas performing arithmetic on potentially string-typed columns
❌ Schema typing inconsistencies in SQLite

Surface enrichment accepted as v2 baseline.
Weather + peaks considered production-stable.

Core Problem Identified

The training dashboard assumes strict numeric typing.

SQLite does not enforce strict typing.

Pandas does not guarantee numeric dtype when reading from SQLite.

Result:
Repeated arithmetic failures.

This will continue unless:

All dashboard queries normalise dtypes immediately after load
OR

SQLite schema is migrated to strict numeric typing
OR

A centralised DB helper enforces dtype casting

This is the root architectural issue.

Recommended Next Direction

Before adding ingestion enrichment:

Stabilise dashboard numeric casting globally

Introduce centralised DataFrame type normalisation

Avoid patching individual arithmetic lines

Lock schema typing explicitly

Validate dashboard end-to-end

Only after dashboard stability is confirmed should ingestion be modified to auto-enrich new workouts.

Ingestion Engine Enrichment + Plot Sample Stabilisation (Feb 2026)
Summary of Work Completed
1️⃣ Ingestion Engine Enrichment (New GPS Workouts)

We extended the ingestion engine to replicate the backfill logic for:

Route context (location + surfaces)

Weather (summary + per-point weather)

Map points (simplified routes)

Plot samples (chart-ready time series)

Moving/stationary time calculation

GAP calculation (partial)

Peak hit framework (wired but not fully stabilised)

The goal was to ensure newly ingested FIT files are enriched automatically, without needing manual backfill scripts.

2️⃣ FIT GPS Coordinate Handling

Confirmed that:

FIT latitude/longitude values are converted correctly from semicircles.

Map rendering is functioning correctly for newly ingested workouts.

Map simplification logic is operational.

3️⃣ Database Path Issue Identified and Fixed

Critical issue discovered:

Two database files existed:

C:\trail_ops\trailops.db

C:\trail_ops\data\trailops.db

At various times:

Training Dashboard and Ingestion Dashboard were pointing to different DB files.

This caused apparent “missing workouts” after ingestion.

Resolution:

Standardised DB path to C:\trail_ops\data\trailops.db.

Updated database module to prevent accidental root-level DB creation.

4️⃣ Plot Sample Generation Failure (Root Cause)

Newly ingested workouts were producing:

900 rows in workout_plot_samples

All numeric columns NULL

Charts showing “No data available”

GAP and moving pace = N/A

Root cause:

Numeric coercion failures from SQLite → pandas

Silent NaN propagation

Existing plot samples not being rebuilt properly

Module import instability preventing reliable backfill execution

Manual rebuild confirmed:

Plot samples can be rebuilt successfully

Charts render when NULL-heavy rows are deleted + regenerated

5️⃣ Moving Time Fix

Moving time was initially calculated incorrectly (0 seconds).

Fix:

Moving time now derived from speed samples

Stationary time recalculated correctly

Moving time appears correctly in dashboard

6️⃣ Remaining Issues

Current state:

Charts render correctly for rebuilt workouts

Moving time correct

Moving pace still not consistently updating

GAP not reliably written to workouts table

Backfill script failing due to import instability:

ModuleNotFoundError: No module named 'app'

This is caused by inconsistent sys.path when running standalone scripts vs Streamlit context.

Current Problem Statement

We are currently stuck on:

Import instability (app vs analysis namespace resolution)

Plot sample + GAP logic needing deterministic rebuild behaviour

Lack of reliable post-ingestion enrichment sweep

Application structure becoming fragile due to:

Multiple dashboards

Multiple entry points

Manual sync + manual ingest workflow

No background automation

🎯 Next Objectives
Immediate

Stabilise plot sample generation permanently

Ensure moving pace and avg GAP always populate

Implement clean backfill for workouts since 2026-02-01 including peak hits

Medium-Term

Simplify architecture:

Single unified dashboard UI with:

Tab 1: Ingestion

Tab 2: Training Dashboard

Single process startup (one Streamlit app, multiple pages)

Background automation:

Auto-scan HealthFit folder

Auto-ingest new workouts

Auto-enrich (plot samples, weather, route context, peaks)

No manual buttons required

Eventually:

Run as background app

System tray icon

No visible PowerShell windows

Phase Update (2026-02-23) — Import Stability + Plot Samples + Enrichment Pipeline

Status: Stable. Dashboards run. New workouts can be enriched deterministically. Plot samples, moving time/pace, and GAP now compute correctly for new ingests.

What was broken

Newly ingested workouts were getting workout_plot_samples rows written (usually 900), but all numeric fields were NULL, resulting in:

Charts showing “No data available”

moving_time_s=0, moving_pace_min_per_mile=NULL, avg_gap_min_per_mile=NULL

Backfill/CLI scripts intermittently failed with:

ModuleNotFoundError: No module named 'app'

Import paths unstable depending on how code was launched (module vs file execution)

Root cause for NULL plot samples was not the maths. It was DB → pandas read behaviour.

Root cause discovered (critical)

app/db/database.py used a custom SQLite row_factory (RowProxy/dict-like rows) for app convenience.

pandas.read_sql_query() does not reliably handle custom dict-like row factories, causing silent dtype/object issues and NaN propagation even when SQLite values were valid REALs.

Result: plot sample generation read “valid” data but computed everything as NaN, then inserted NULL-heavy rows.

Fixes implemented (permanent)

Database connection now supports row-factory modes

get_connection(row_factory="proxy" | "tuple" | "sqlite_row")

Default remains "proxy" for existing app code.

Pandas-safe reads must use "tuple" mode (or helper get_pandas_connection() if present).

This prevents pandas from silently poisoning dataframes with NaNs.

Plot sample pipeline repaired

Plot sample generation now reads SQLite via a pandas-safe connection (tuple rows).

Rebuild of workout 2234 confirmed:

workout_plot_samples: 900 rows, 0 NULL pace/grade/gap

workouts: moving time + moving pace + avg GAP populated correctly

Import stability cleanup

Multiple modules were updated earlier in this phase to consistently import via app.* namespace.

Correct execution pattern is now: python -m app.<module> for tools/scripts (avoids ModuleNotFoundError).

New addition: deterministic enrichment pipeline

New file: app/analysis/enrichment_pipeline.py

Introduces enrich_workout(workout_id) that runs enrichment in a single, idempotent pass:

map points

route context

surfaces

weather

peaks (can return False legitimately if no hits)

plot samples (includes moving time/pace + GAP)

Updated: app/ingestion/fit_ingestor.py now calls the enrichment pipeline rather than ad-hoc enrichment logic.

Validation performed

Idempotency confirmed:

Running enrich_workout(2234) twice did not create duplicate rows

Row counts stable (map_points and plot_samples)

Batch smoke test:

enrich_workout() run across last 20 workouts → bad: 0

Backfill using pipeline:

Successfully enriched all workouts since 2026-02-01 with no errors

Remaining NULL moving pace/GAP rows were non-running workout types (expected)

Key workflow rule (important for future chats)

Always run tools/scripts as modules from repo root:

cd C:\trail_ops

.venv\Scripts\activate

python -m app.tools.<script_name>

Never run scripts by absolute file path if they import app.*

Current stable baseline

Dashboards run

Plot samples are trustworthy again

GAP + moving time/pace calculate correctly for running workouts

Enrichment pipeline exists and is the single source of truth for post-ingestion enrichment

Unified Dashboard + Automation + System Tray Phase — 2026-02-24
Purpose of This Phase

Stabilise architecture and reduce fragility after enrichment pipeline success by:

Removing legacy file confusion

Unifying dashboards into a single entrypoint

Standardising DB access patterns

Introducing background automation

Moving backend execution to a controlled system tray process

Improving debug visibility

This phase focused on architectural clean-up and operational stability, not feature expansion.

What Was Completed
1️⃣ Architecture Clean-Up (Fragility Reduction)

Objective: Remove ambiguity and prevent accidental imports.

Completed:

Archived all *-OLD.py files outside package path

Backed up full trail_ops directory

Standardised module execution pattern:

python -m app.tools.<script>

Eliminated direct script path execution

Standardised DB access:

pandas reads must use tuple row_factory

Enforced via get_connection(row_factory="tuple")

This prevents silent dtype corruption and future plot_sample failures.

Architecture is now significantly less brittle.

2️⃣ Unified Streamlit Application

Old State:

Separate ingestion dashboard

Separate training dashboard

Multiple entrypoints

Confusing execution patterns

New State:
Single Streamlit app:

streamlit run app/main_app.py

With tabs:

Training (Home)

Debug / Tools (includes ingestion)

System tray handles backend process

All ingestion controls moved into Debug tab.

This removes multi-app complexity and prevents DB desync.

3️⃣ Debug / Tools Overhaul

Debug tab now includes:

Environment Snapshot

Python version

OS

Working directory

App directory

DB path

DB size

Database Health Checks

workout counts

sample counts

plot_samples count

map_points count

Missing-data warnings

Missing Data Explorer

Filter workouts missing:

plot_samples

map_points

GAP

moving_time

peaks

Includes:

Interactive workout table

Backfill buttons scoped to filtered list

Backfill Controls

Buttons for:

Rebuild plot_samples (moving + GAP)

Backfill map_points

Backfill peaks (via enrichment pipeline)

All run safely inside app context.

Auto Ingest Status Panel

Manual sync + ingest button

Last run status

auto_ingest.log viewer

Newest logs at top

Fixed height with scroll

Log download button

Recent Workouts + Enrichment Coverage Table

Shows last 50 workouts with:

Location present?

Weather present?

Surface stats present?

Peaks present?

Allows instant visual validation of enrichment quality.

4️⃣ Automated HealthFit Ingestion

Implemented:

app/tools/auto_ingest_once.py

Runs:

HealthFit sync

FIT ingest

enrichment_pipeline.enrich_workout()

Configured via:

Windows Task Scheduler
(5 minute interval)

Hidden execution via:

wscript + VBS launcher

No console window appears.

Result:

New workouts are automatically:

Ingested

Enriched

Plot samples generated

GAP + moving pace computed

Peaks + surfaces + weather attached

Manual ingestion is no longer required.

5️⃣ System Tray Backend Controller

Implemented:

app/tools/tray_streamlit.py

Features:

System tray icon

Start server

Stop server

Restart server

Open dashboard

Open server log

Quit

Icon states:

Green = running

Red = stopped

Red + tooltip = crashed

Crash monitoring:

Detects unexpected Streamlit exit

Displays Windows toast notification

Updates tray state

Runs windowless via:

pythonw -m app.tools.tray_streamlit

No PowerShell window.

Streamlit now behaves like a proper local desktop service.

Current Stable Baseline

✔ Unified dashboard
✔ Deterministic enrichment pipeline
✔ Automated ingestion
✔ Background execution
✔ System tray process control
✔ Crash notification
✔ Debug visibility
✔ Backfill from UI
✔ Stable DB access pattern

TrailOps now operates like a structured local application, not a collection of scripts.

React Migration & Mission Console UI Overhaul (FastAPI Era)
1️⃣ Architecture Shift: Streamlit → FastAPI + React
Previous State (Streamlit Era)

UI rendered entirely in Streamlit.

Direct SQLite reads inside Streamlit.

Plotly charts embedded directly.

Route maps rendered via Streamlit components.

Logic and UI tightly coupled.

Current Architecture (Post-Migration)

We have moved to a separated frontend/backend model:

Backend

FastAPI application running on 127.0.0.1:8000

Entry: python -m app.tools.run_api

Health check endpoint:
/health → {"status":"ok"}

Backend Capabilities

Serves:

Workout summaries

Topline aggregates

Daily rollups

Basic peaks data

CORS enabled for React frontend (localhost:5173)

Designed for expansion to:

Per-workout time series endpoints

Peaks classification endpoints

AI coach interaction endpoints

Frontend

Vite + React (local dev on port 5173)

Structured dashboard layout modeled after mission console mock

Fully separated from backend logic

Communicates via REST fetch calls to FastAPI

2️⃣ FastAPI Server Improvements Since Streamlit
✅ Added

Dedicated API server process

CORS middleware for React compatibility

Structured endpoint separation (health, workouts, stats)

Port conflict handling awareness

Batch start scripts (TrailOps_Start_All.bat)

System tray / API dual-mode boot support

✅ Stability Fixes

Socket binding error resolution

Port conflict detection

Clear separation between Streamlit legacy and new React flow

⚙️ Design Direction

Backend now serves data only.
All UI rendering logic lives in React.

This enables:

Clean UI iteration without touching ingestion logic

Proper AI layer integration later

Modular analytics endpoints

3️⃣ React Dashboard Phases Completed
Phase 0 – Scaffold

Vite project setup

React mount confirmation

API connectivity verified

Phase 1 – Mission Shell Layout

Top KPI stat row

Alerts & Anomalies bar

Primary Telemetry chart

Recent Activity + AI Coach split layout

Peaks placeholder section

Snapshot placeholder section

Recent Workouts scroll section

Phase 2 – UI Refinement & Console Styling

Mission console dark theme

Retro-futuristic glow

Subtle blue-tinted background

Card surfaces with gradients

Header typography tightening

Mono-style font adoption

KPI card redesign

Hover glow states

Phase 2A–2I UI Enhancements

Date range picker with presets + custom calendar

Forced orange styling (react-day-picker overrides)

Sport filter dropdown

Section capitalization consistency

Peaks section rebuilt:

Class dropdown (Wainwrights default)

Class trackers (Wainwrights, Dodds etc placeholders)

Top visited peaks list

Peak hits heatmap block grid

Snapshot section repositioned under Peaks

Peaks + Snapshot full-width layout

Sparkline placeholders in Recent Workouts:

Stat selection per workout type:

Running outdoor → Elevation if >800ft else Pace

Running indoor → HR

Walking outdoor → Elevation

Walking indoor → HR

Hike → Elevation

Cycling → Power

Strength/HIIT/Stair → HR

Color mapping:

HR → red

Pace → yellow

Elevation → purple

Power → blue

4️⃣ Current State
Working

FastAPI stable

React dashboard stable

Layout matches mission mock ~80–85%

UI structure finalized

Peaks UI shell complete

Sparkline UI shell complete

Not Yet Wired

AVG HR in recent activity

GAP wiring

Real per-workout time series

Peaks classification data endpoint

Class progress calculations

AI Coach real logic

Primary telemetry configuration controls

Per-card hover explainers

Real chart configurability (main metric + overlay)

5️⃣ Upcoming Phases
Phase 3 – Data Wiring Layer

Goal: Replace placeholders with real API-driven data.

Tasks:

Add endpoint for per-workout time series:

/workouts/{id}/samples

Returns: timestamp, hr, pace, elevation, cadence, power

Replace sparkline fake arrays with real values.

Wire AVG HR + GAP correctly.

Add peaks classification endpoint:

/peaks/classifications

/peaks/progress?class=wainwrights

Move 800ft logic to backend.

Phase 4 – Telemetry Engine Upgrade

Main metric selector

Overlay selector

Smoothing toggle

Aggregation toggle (daily / weekly / monthly)

Performance optimization (memoization)

Phase 5 – AI Coach Integration

Conversation memory store (SQLite table)

Natural filter command parsing

Range switching from chat

Automated anomaly commentary

Injury tracking table

Phase 6 – Performance & Polish

Debounced filtering

Virtualized workout list

Map component integration (Leaflet or Mapbox)

Chart animation smoothing

Microinteraction polish

Phase Update (2026-02-26) — FastAPI Context Wiring + Recent Activity Stabilisation
Status

Backend stable. Context endpoint functioning. Peaks deduped. Location formatting refined. Weather not yet rendering in UI despite DB presence. React UI ~85–90% complete.

What Was Completed in This Phase
1️⃣ FastAPI Context Endpoint Implementation

Implemented new endpoint:

GET /workouts/{workout_id}/context

Purpose:
Provide all enrichment metadata required by the React “Recent Activity” card in a single payload.

Returns:

Location (trimmed to “Locality, Region”)

Surface (if present)

Weather snapshot

Peaks list (deduped)

Peaks count

This endpoint replaces placeholder UI logic and removes direct frontend DB assumptions.

2️⃣ React Recent Activity Upgrade

Recent Activity component was rebuilt to:

Add stat cards:

Distance

Time

Moving Time (HH:MM:SS)

Avg HR (with bpm suffix)

Avg Moving Pace

Avg GAP

Elevation

Reposition layout:

Activity selector (top-left)

Workout ID moved under selector (reduced size)

Route map block moved above stat grid

Context pills aligned on same row as formatted date/time

Date formatting updated:

HH:MM, Day DD Month

Removed instruction line:
“Pick any activity from the last 30 days.”

3️⃣ Peak Duplication Bug Resolved

Issue:
Peaks appearing multiple times in UI (e.g., Arnside Knott x4).

Root cause:
Multiple OSM IDs with same name.

Resolution:
Deduped by (peak_name, elevation) before returning to frontend.

Result:
Peaks count now reflects unique summit visits.

4️⃣ Database Column Introspection Fix

Critical crash discovered:

AttributeError: 'sqlite3.Row' object has no attribute 'get'

Root cause:
Column inspection helper _cols() assumed dict-like rows.

Fix:
Adjusted _cols() to read PRAGMA results via index access:

row[1]

This stabilised context endpoint.

5️⃣ Weather Data Investigation

Weather rows confirmed present in DB:

workout_weather contains valid entries.

Schema (important):

start_temp_c

start_precip_mm

start_wind_kph

start_weather_code

temp_c

wind_kph

precip_mm

weather_code

point_type

obs_time_utc

However:

Weather is not rendering in UI.

Current suspected causes:

Query selecting incorrect weather columns

Not filtering correctly by point_type='start'

Weather row ordering returning non-start row first

Frontend conditional rendering suppressing nullish fields

Weather remains the only enrichment block not displaying.

Current Stable Baseline

✔ FastAPI server stable
✔ React frontend stable
✔ Context endpoint functional
✔ Peaks deduped
✔ Location trimmed correctly
✔ Moving time and duration formatted properly
✔ AVG HR displays with bpm
✔ Workout selector extended to 30 days

❗ Weather not yet visible
❗ Surface pill not yet verified

Immediate Next Objective

Fix weather rendering end-to-end:

Inspect actual weather rows for representative workout

Adjust backend query to:

Prefer point_type='start'

Fallback to earliest obs_time_utc

Select correct column names from actual schema

Validate JSON payload manually via curl

Confirm frontend rendering logic

Only after weather is confirmed stable should we proceed.

Next Phase After Weather — Maps Integration

Phase: React Map Component Integration

Goal:
Replace placeholder route map with live interactive component.

Options:

Leaflet (offline friendly, lightweight)

Mapbox GL (better visuals, heavier)

Plain SVG (minimalist, fastest)

Requirements:

Draw simplified route polyline from map_points

Fit bounds to route

Show start/end markers

Optionally highlight peaks hit

Must remain local-first

Map integration should reuse existing map_points table generated by enrichment pipeline.

Phase Update (2026-02-28) — Moon Backfill + React Map (Leaflet) Integration

Status: Mostly working. Map renders (dark default) with orange route line. Moon data is now present in DB. Remaining gaps: moon pill intermittently missing in UI (frontend/context wiring), and mile markers not yet implemented on the map. 

TrailOps_Canonical_Project_Cont…

What Was Completed in This Phase

1️⃣ Moon Data Investigation + Backfill (DB now populated)

Confirmed workout_weather had rows but moon fields were entirely NULL.

Implemented and ran a backfill to populate:

moon_phase_name

moon_illumination

moon_phase

Verified result via SQL (example: workout_id=2196 shows “Waning Crescent” + illumination).

2️⃣ FastAPI Context Moon Surfacing (Defensive Patch)

Added a defensive layer to GET /workouts/{workout_id}/context so moon fields are pulled directly from workout_weather if older context query versions don’t surface them. 

server

3️⃣ React Route Map (Leaflet) Added to Recent Activity

Added a Leaflet map to the Recent Activity panel (new “ROUTE MAP” card).

Uses existing backend endpoint:

GET /workouts/{workout_id}/map-points?level=1&max_points=3000&include_markers=true

Route draws successfully and is styled in TrailOps orange.

Added dark/light base map toggle (default: dark).

Map height set via .routeMapCanvas in CSS.

4️⃣ Build/Compile Fixes During Map Merge

Fixed a PostCSS failure (Unclosed block) originating in styles.css around the route map styles.

Fixed JSX compile failures caused by mismatched JSX grouping/placement near the Recent Activity section (errors around the “Route map” comment insertion).

What’s Currently Broken / Not Finished

Moon pill not reliably showing in the UI, even though DB + API support now exists. Likely a frontend conditional/render path or context-shape mismatch (moon nested under recentCtx.weather). 

server

Mile markers along the route are not implemented yet (desired: numbered markers every mile).

Map fit/bounds padding still needs refinement (route appears too small in frame; reduce padding / increase zoom slightly).

Start/end markers were requested to be clearer (green start, red end). Current state varies by build and needs confirming.

Immediate Next Step (Carry Into Next Chat)

Fix moon pill rendering path so it always displays when moon_phase_name + moon_illumination exist in context payload.

Add mile markers to Leaflet map (front-end marker generation based on distance_m).

Tighten fitBounds() padding so route fills the card more naturally.

Peaks Dashboard Stabilisation & UI Refactor Attempt (March 2026)
Context

After stabilising the FastAPI backend and resolving multiple API crashes (sqlite3.Row.get, incorrect return indentation, API down states), focus shifted to the Peaks – Lists – Progress section in the React frontend.

The objective was to:

Finalise peak + POI data rendering

Add class progress trackers (X / total per class)

Improve heatmap weekday alignment

Add click-based diagnostics drawer

Constrain “Top Visited” lists to fixed-height scroll boxes

Improve route map smoothing

Preserve a known-good baseline before further UI refactoring

What Was Completed
1️⃣ Backend Stability

/peaks/dashboard endpoint stabilised.

Fixed sqlite3.Row misuse (.get() → proper indexing).

API server confirmed loading from:

C:\trail_ops\app\api\server.py

C:\trail_ops\app\api\queries.py

C:\trail_ops\app\api\map_points.py

Diagnostics endpoint /peaks/item/{peak_osm_id} implemented to support click modal.

Confirmed data pulling correctly from peaks and workout_peak_hits.

2️⃣ Frontend Adjustments Attempted

POIs moved directly under heatmap to remove empty space.

Diagnostics modal added (click on peak/POI row).

Heatmap adjusted to Monday-aligned layout.

Attempted fixed-height scrollable “Top Visited” panels (top 10 only).

API client refactored to remove API_BASE undefined errors.

Multiple build failures resolved (fetchPeakItem export mismatch, broken template strings, missing exports).

3️⃣ Regressions Encountered

Diagnostics modal intermittently broken due to:

API_BASE undefined

Missing fetchPeakItem export

api.js inconsistencies

Rollup/Vite build failures due to malformed template strings and export mismatches.

Heatmap weekday alignment inconsistently corrected.

Fixed-height “Top Visited” panels regressed back to full-length lists.

Click handlers lost again after patch cycles.

Patch layering introduced frontend instability.

Current State (Baseline at End of Chat)

Dashboard loads successfully.

Backend peaks data stable.

Class tracker logic partially present but visually inconsistent.

Top visited lists not constrained to fixed height.

Clickable diagnostics not functioning reliably.

Heatmap weekday alignment uncertain.

Key Lessons

Patching frontend incrementally without re-evaluating full file context leads to regressions.

api.js must remain the single source of truth for API access.

App.jsx has become too large (~74KB) and is brittle to partial patching.

Frontend requires controlled refactor rather than additive patching.

Every patch must:

Include full file replacements

Include backup step

Include cache clear

Include build verification

Delete staging files after confirmation

Next Direction

Move Peaks section refactor into clean chat with:

Full file review

Structural redesign plan

Controlled implementation sequence:

Stabilise api.js

Stabilise heatmap logic

Re-implement diagnostics click logic cleanly

Implement fixed-height scroll containers

Implement class tracker progress bars

Add future peaks map visualisation

No more patch stacking.

Peaks Backend Crash Fix + Peaks Dashboard Recovery (March 2026)
Context

Peaks dashboard was hard-crashing the FastAPI backend (500s) even though the rest of the dashboard loaded. The UI was calling:

GET /peaks/dashboard?range=30d&cls=wainwrights

…and the API was dying in get_peaks_dashboard() due to incorrect assumptions about sqlite row objects.

What Was Broken

1️⃣ Backend import crash (API wouldn’t start)

ImportError: cannot import name 'rename_peak_item' from 'app.api.queries'

server.py imported rename_peak_item but queries.py didn’t reliably define/export it (or naming mismatch existed).

2️⃣ Peaks dashboard 500 error (API starts, peaks endpoint fails)

Error:

AttributeError: 'sqlite3.Row' object has no attribute 'get'

Root cause:

Code treated sqlite3.Row like a dict (row.get(...)) when sqlite rows only support row["col"] + row.keys().

What We Changed / Fixed

1️⃣ Made /peaks/dashboard robust against sqlite3.Row

Removed all .get() usage on sqlite rows inside the peaks dashboard flow.

Standardised row access using safe indexing and key checks.

Result: /peaks/dashboard returns successfully instead of 500.

2️⃣ Repaired rename plumbing so API can load cleanly

Ensured rename_peak_item(...) exists in queries.py and is imported correctly by server.py.

Confirmed the rename endpoint is present:

POST /peaks/item/{peak_osm_id}/rename

Result: API boot no longer fails on import.

3️⃣ Confirmed Peaks dashboard now loads

Backend now responds successfully to:

GET /peaks/dashboard?range=30d&cls=wainwrights

Dashboard loads and Peaks section is “working” again (baseline restored).

Files Touched (Backend)

C:\trail_ops\app\api\queries.py

C:\trail_ops\app\api\server.py

Current State (End of Chat)

✅ API runs reliably
✅ Peaks endpoint no longer crashes with sqlite row errors
✅ Dashboard loads and Peaks data returns again

⚠️ Still known issues (not solved in this chat)

Frontend still sometimes calls range=[object Object] in requests (React state/object being passed instead of string), which will cause inconsistent behaviour even if backend is stable.

Peaks UI still has “other problems” to resolve later (you paused here for sanity).

Immediate Next Step (Carry Into Next Chat)

Fix React request shaping so peaks dashboard always calls:

range=7d|30d|12m|all (string only)

Audit Peaks UI behaviour now that backend is stable:

verify Top Visited fixed height panels

verify peak/POI row click behaviour (if present)

verify class tracker UI + heatmap alignment

## Future Roadmap (Headlines Only)
- Training load modelling
- Shoe lifecycle analytics
- Performance trend analysis
- UI / UX refinement
- AI coach integration (local only)

TrailOps – Future Planning Mode (Parked)
Objective

Extend TrailOps beyond retrospective activity analysis to support structured planning of future hikes and trail/fell runs using imported GPX routes, integrated weather forecasting, and AI-assisted route planning.

This will allow planned routes to be logged, analysed, and later matched to completed FIT workouts for plan-vs-execution comparison.

Scope (Planned Architecture)
1. GPX Route Planning (Import-Based)

Routes will not be built inside TrailOps.
Users will create routes externally (e.g., Outdooractive, OS Maps) and import GPX files.

For each imported GPX route:

Store canonical route metadata:

Distance

Ascent / descent

Min / max elevation

Steepness distribution

Bounding box

GPX hash (for duplicate detection)

Render:

Interactive map polyline

Elevation profile

Route statistics summary

2. Weather Snapshot System

Weather is considered a critical component of route planning.

For each route plan:

Pull forecast data at time of planning

Store forecast snapshot (not dynamic updates)

Sample forecast along route at representative points

Include:

Temperature

Wind speed and gusts

Precipitation

Cloud cover

Humidity

Store model/provider metadata and timestamp

Generate route-level summaries:

Worst-case wind/gust

Coldest feels-like

Wettest hour

Risk flags (wind exposure, cold + wet, etc.)

3. Mountain & Elevation Context

Identify peaks near route using OSM data

Highlight significant peaks (elevation threshold)

Approximate forecast at high elevation points

Flag exposed or high-risk sections

4. Sun & Daylight Context

For planned start date/time:

Sunrise

Sunset

Civil twilight

Estimated duration

Daylight buffer

Suggested hard turnaround time

5. AI Coach Planning Context

Each route plan will generate a structured RoutePlanContext including:

Route stats

Weather summary

Elevation/exposure

Daylight window

User state:

Current injuries

Training block

Upcoming races

Recent load

The AI Coach can generate:

Pack list

Pacing plan

Risk assessment

Contingency options

Turnaround recommendations

Structured outputs will be stored alongside raw chat logs.

6. Plan Lifecycle & Matching

Route plans will have status:

planned

scheduled

attempted

completed

skipped

Upon FIT ingestion:

Attempt automatic plan → workout matching

Store match confidence

Allow manual override

This enables plan vs actual comparison.

7. Persistent Memory Structures (Planned)

New database entities (conceptual):

route_plans

route_plan_weather_snapshots

route_plan_sun_data

route_plan_summary

plan_workout_links

injuries (timeline-based)

races

training_blocks

post_activity_reviews

gear_inventory (optional)

Rationale

Planning mode aims to:

Integrate route + weather + personal state into one decision context

Improve safety (weather, daylight, exposure)

Improve decision quality over time via plan-vs-execution comparison

Enable AI Coach to provide context-aware recommendations

Route Receipt (Receipt Printer Output)

Add optional “Print Route Receipt” action once a route plan is complete.

Output a thermal-receipt-style layout (supermarket vibe) containing:

Route name + plan ID + date/time planned

Start location + planned start time

End location + estimated finish time + turnaround time (if set)

Distance, ascent/descent, max elevation

Peaks/waypoints list with elevations

Weather snapshot summary (valley + high point): temp range, wind/gusts, precip risk

Sunrise/sunset + daylight buffer

Key warnings/flags (wind exposure, cold/wet, steep descent, etc.)

Small “route outline” mini-map (very simplified line art) + QR code linking to the plan in TrailOps

Implementation notes:

Generate ESC/POS-compatible output (common receipt printers).

Render as monospaced text + optional raster image (for the mini-map/QR) depending on printer support.

Keep it fully offline-capable if desired (local printing via USB/Bluetooth where possible).

# Status
Parked for future implementation.
Not currently in active roadmap.

---

## How New Chats Should Be Started
- This markdown must be pasted **at the start** of every new TrailOps chat
- It is the **authoritative source of truth**
- Anything not defined here must not be assumed
- Any proposed deviations must be explicitly discussed before implementation