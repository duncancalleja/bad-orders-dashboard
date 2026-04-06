from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components


ROOT = Path(__file__).resolve().parent


def _read_first_existing(paths: list[Path]) -> str | None:
    for p in paths:
        try:
            if p.is_file():
                return p.read_text(encoding="utf-8")
        except Exception:
            continue
    return None


def _build_live_dashboard_html(*, country_code: str, year: int) -> tuple[str, str]:
    out_dir = Path(tempfile.gettempdir()) / "bad-orders-dashboard"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{country_code.lower()}_bad_orders_{year}.html"

    cmd = [
        sys.executable,
        str(ROOT / "build_bad_orders_dashboard.py"),
        "--country-code",
        country_code.lower(),
        "--year",
        str(int(year)),
        "--output",
        str(out_path),
    ]
    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        env=os.environ.copy(),
        text=True,
        capture_output=True,
    )
    logs = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    if proc.returncode != 0:
        raise RuntimeError(logs.strip() or f"Dashboard build failed (exit={proc.returncode}).")

    html = out_path.read_text(encoding="utf-8")
    return html, logs.strip()


st.set_page_config(page_title="Bad Orders Dashboard", layout="wide")
st.title("Bad Orders Dashboard")
st.caption("Provider-at-fault bad orders • Brand = vendor_name")

mode = st.radio(
    "Mode",
    options=["Static snapshot (from repo)", "Live (rebuild from Databricks)"],
    horizontal=True,
)

static_candidates = [
    ROOT / "docs" / "index.html",
    ROOT / "dashboard.html",
    ROOT / "index.html",
]

if mode == "Static snapshot (from repo)":
    html = _read_first_existing(static_candidates)
    if not html:
        st.error(
            "No static dashboard found. Add one of these files to the repo: "
            + ", ".join(str(p.relative_to(ROOT)) for p in static_candidates)
        )
    else:
        components.html(html, height=1200, scrolling=True)

else:
    token_present = bool(os.environ.get("DATABRICKS_TOKEN", "").strip())
    if not token_present:
        st.error(
            "Missing environment variable `DATABRICKS_TOKEN`.\n\n"
            "Ask `#boltable-support` to set it as a runtime secret/env var for this app."
        )

    col1, col2 = st.columns([1, 1])
    with col1:
        country_code = st.selectbox("Country", options=["mt"], index=0)
    with col2:
        year = st.selectbox("Year", options=[2026], index=0)

    do_refresh = st.button("Build / Refresh now", type="primary", disabled=not token_present)

    if do_refresh:
        with st.spinner("Querying Databricks and building dashboard…"):
            try:
                html, logs = _build_live_dashboard_html(country_code=country_code, year=int(year))
            except Exception as e:
                st.error(str(e))
            else:
                st.session_state["live_html"] = html
                st.session_state["live_logs"] = logs

    live_html = st.session_state.get("live_html")
    if live_html:
        components.html(live_html, height=1200, scrolling=True)
        with st.expander("Build logs"):
            st.code(st.session_state.get("live_logs", ""), language="text")
