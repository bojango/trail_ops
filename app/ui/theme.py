from __future__ import annotations

import streamlit as st


def inject_theme() -> None:
    """Mission-console theme (Phase 1D: visual polish).

    Focus:
    - Reduce "Streamlit default" vibes (spacing + widget chrome).
    - Improve chart embedding so matplotlib doesn't scream white.
    - Improve right-rail details typography.
    """
    css = r"""
    <style>
      :root{
        --bg0:#05060a;
        --bg1:#0b0d12;
        --panel:#0c0f16;
        --panel2:#0a0c10;
        --stroke:rgba(255,255,255,.10);
        --stroke2:rgba(255,255,255,.16);
        --text:rgba(255,255,255,.90);
        --muted:rgba(255,255,255,.62);
        --muted2:rgba(255,255,255,.42);
        --accent:#ff8a1f;
        --accent2:#ffb067;
        --good:#21d07a;
        --bad:#ff375f;
        --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono","Courier New", monospace;
        --sans: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji";
        --r:16px;
      }

      html, body, [data-testid="stAppViewContainer"]{
        background: radial-gradient(1200px 800px at 20% -10%, rgba(255,138,31,.10), transparent 55%),
                    radial-gradient(900px 600px at 95% 0%, rgba(96,165,250,.06), transparent 55%),
                    linear-gradient(180deg, var(--bg0), var(--bg1));
        color: var(--text);
      }

      .block-container{
        padding-top: 0.60rem;
        padding-bottom: 2rem;
        max-width: 1600px;
      }

      #MainMenu, footer { visibility: hidden; }
      header { visibility: hidden; }

      .to-shell-topbar, .to-panel{
        border: 1px solid var(--stroke);
        background: linear-gradient(180deg, rgba(255,255,255,.06), rgba(255,255,255,.02));
        border-radius: var(--r);
        box-shadow: 0 10px 40px rgba(0,0,0,.35);
        position: relative;
        overflow: hidden;
      }
      .to-shell-topbar{ padding: 14px 16px; }
      .to-panel{ padding: 12px 12px; }
      .to-shell-topbar:before, .to-panel:before{
        content:"";
        position:absolute; inset:0;
        background: repeating-linear-gradient(180deg, rgba(255,255,255,.02) 0, rgba(255,255,255,.02) 1px, transparent 1px, transparent 4px);
        opacity:.26;
        pointer-events:none;
      }

      .to-panel-title{
        font-family: var(--mono);
        font-size: 12px;
        color: rgba(255,255,255,.70);
        letter-spacing:.10em;
        text-transform: uppercase;
        margin: 0 0 10px 0;
      }
      .to-muted{ color: var(--muted); }

      .to-brand{ display:flex; gap:12px; align-items:center; }
      .to-logo{
        width:36px; height:36px; border-radius:10px;
        background: radial-gradient(12px 12px at 30% 30%, rgba(255,138,31,.65), transparent 60%),
                    linear-gradient(135deg, rgba(255,255,255,.12), rgba(255,255,255,.03));
        border:1px solid var(--stroke2);
        box-shadow: inset 0 0 0 1px rgba(255,255,255,.06);
      }
      .to-title{
        font-family: var(--mono);
        letter-spacing:.08em;
        text-transform: uppercase;
        font-size: 14px;
        margin:0;
        color: rgba(255,255,255,.92);
      }
      .to-subtitle{
        font-family: var(--sans);
        margin:0;
        font-size: 12px;
        color: var(--muted);
      }

      .to-chip-row{ display:flex; flex-wrap:wrap; gap:8px; justify-content:flex-end; }
      .to-chip{
        border:1px solid var(--stroke);
        background: rgba(0,0,0,.18);
        border-radius: 999px;
        padding: 6px 10px;
        font-family: var(--mono);
        font-size: 12px;
        color: rgba(255,255,255,.85);
        white-space: nowrap;
      }
      .to-chip b{ color: rgba(255,255,255,.62); font-weight: 500; margin-right: 6px; }
      .to-chip-accent{ border-color: rgba(255,138,31,.35); background: rgba(255,138,31,.10); }

      .to-statgrid{ display:grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; }
      .to-stat{
        border:1px solid var(--stroke);
        background: rgba(0,0,0,.14);
        border-radius: 14px;
        padding: 12px 12px;
      }
      .to-stat .k{ font-family: var(--mono); font-size: 11px; letter-spacing:.10em; text-transform: uppercase; color: rgba(255,255,255,.64); }
      .to-stat .v{ margin-top: 8px; font-family: var(--mono); font-size: 22px; color: rgba(255,255,255,.92); }

      /* Right-panel key/values */
      .to-kv{ display:grid; grid-template-columns: 1fr; gap:10px; }
      .to-kv .row{ display:flex; justify-content:space-between; gap:10px; }
      .to-kv .k{ font-family: var(--mono); font-size: 11px; letter-spacing:.10em; text-transform: uppercase; color: rgba(255,255,255,.55); }
      .to-kv .v{ font-family: var(--sans); font-size: 13px; color: rgba(255,255,255,.88); text-align:right; }

      /* Widget chrome trims */
      div[data-baseweb="select"] > div{
        border-radius: 12px !important;
        background: rgba(0,0,0,.18) !important;
        border-color: rgba(255,255,255,.10) !important;
        min-height: 38px;
      }
      label, .stSelectbox label, .stRadio label{
        color: rgba(255,255,255,.62) !important;
        font-family: var(--mono) !important;
        letter-spacing: .06em !important;
        text-transform: uppercase !important;
        font-size: 11px !important;
      }

      /* Radio nav: hide circle, make it pill-like */
      div[data-testid="stRadio"] div[role="radiogroup"]{ gap: 8px; }
      div[data-testid="stRadio"] label{
        border: 1px solid rgba(255,255,255,.10);
        background: rgba(0,0,0,.14);
        border-radius: 12px;
        padding: 10px 12px;
      }
      div[data-testid="stRadio"] label:hover{
        border-color: rgba(255,255,255,.18);
        background: rgba(255,255,255,.06);
      }
      div[data-testid="stRadio"] input{ display:none !important; }
      div[data-testid="stRadio"] svg{ display:none !important; }

      /* Matplotlib figures: try to kill white gutters */
      [data-testid="stImage"], [data-testid="stPlotlyChart"], [data-testid="stPyplot"]{
        background: rgba(0,0,0,.08);
        border: 1px solid rgba(255,255,255,.08);
        border-radius: 14px;
        padding: 8px;
      }

      h1, h2, h3{ font-family: var(--mono) !important; letter-spacing: .06em; }
      .stMarkdown p{ margin-bottom: 0.55rem; }
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)
