from __future__ import annotations

# TrailOps unified Streamlit app
# Entry: streamlit run app/main_app.py

# --- Import bootstrap ---------------------------------------------------------
# Streamlit executes this file with sys.path set to the script directory (app/),
# which breaks absolute imports like `from app.config import ...`.
# We add the project root to sys.path so `import app` works reliably.
import sys
from pathlib import Path as _Path

_PROJECT_ROOT = _Path(__file__).resolve().parents[1]  # .../trail_ops
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
# -----------------------------------------------------------------------------

import streamlit as st
import traceback

from app.db.database import init_db

from app.ui.theme import inject_theme
from app.ui.shell import render_app_shell
from app.ui.state import init_global_state, get_nav_page
from app.pages.dashboard import render_dashboard_page
from app.pages.activities import render_activities_page
from app.pages.planning import render_planning_page
from app.pages.system import render_system_page


def _route() -> None:
    """Route to the selected page renderer."""
    page = get_nav_page()
    pages = {
        "Dashboard": render_dashboard_page,
        "Activities": render_activities_page,
        "Planning": render_planning_page,
        "System": render_system_page,
    }
    renderer = pages.get(page, render_dashboard_page)
    renderer()


def main() -> None:
    st.set_page_config(page_title="TrailOps", page_icon="⛰️", layout="wide", initial_sidebar_state="collapsed")

    # Ensure DB exists early (keep this guardrail stable)
    try:
        init_db()
    except Exception as e:
        st.error(f"Database init failed: {e}")
        st.code(traceback.format_exc())
        st.stop()

    inject_theme()
    init_global_state()

    # App shell (top bar + nav + right rail). Body routed inside.
    render_app_shell(body_renderer=_route)


if __name__ == "__main__":
    main()
