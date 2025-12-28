from __future__ import annotations

import base64
import hashlib
import hmac
import json
import re
from datetime import datetime, timedelta, timezone, time, date
from typing import Any

import altair as alt
import gspread
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from dateutil import parser as dtparser
from zoneinfo import ZoneInfo
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

from app.crypto import encrypt_str
from app.gspread_retry import gcall
from app.sheets_client import SheetsClient
from app.spotify_auth import build_auth_url, exchange_code_for_token, get_spotify_user_id
from common.config import load_settings
from worker.app_state import read_app_state, write_app_state_kv
from worker.registry import (
    REGISTRY_TAB,
    ensure_registry_headers,
    find_sheet_by_spotify_user_id,
    upsert_registry_user,
)
from worker.user_sheet import ensure_user_sheet_initialized

# -----------------------------
# Page config + Spotify-ish theme
# -----------------------------
st.set_page_config(page_title="Spotify Logger", page_icon="🎧", layout="wide")

SPOTIFY_GREEN = "#1DB954"
SPOTIFY_BG = "#121212"
SPOTIFY_CARD = "#181818"
SPOTIFY_TEXT = "#FFFFFF"
SPOTIFY_MUTED = "#B3B3B3"
SPOTIFY_BORDER = "#2A2A2A"

# Cover size on Top 5 tabs
COVER_W = 150

st.markdown(
    f"""
<style>
.stApp {{
  background: radial-gradient(1200px 800px at 20% 0%, #1a1a1a 0%, {SPOTIFY_BG} 55%);
  color: {SPOTIFY_TEXT};
}}
h1, h2, h3, h4 {{
  letter-spacing: -0.02em;
}}
.spotify-card {{
  background: {SPOTIFY_CARD};
  border: 1px solid {SPOTIFY_BORDER};
  border-radius: 16px;
  padding: 16px 18px;
}}
.kpi {{
  font-size: 28px;
  font-weight: 800;
  margin: 2px 0 2px 0;
}}
.kpi-label {{
  color: {SPOTIFY_MUTED};
  font-size: 13px;
}}
.badge {{
  display: inline-block;
  padding: 3px 10px;
  border-radius: 999px;
  font-size: 12px;
  border: 1px solid {SPOTIFY_BORDER};
  background: rgba(255,255,255,0.03);
  color: {SPOTIFY_MUTED};
}}
.success-pill {{
  display: inline-block;
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 12px;
  background: rgba(29,185,84,0.12);
  border: 1px solid rgba(29,185,84,0.35);
  color: {SPOTIFY_GREEN};
}}
.stButton > button {{
  border-radius: 999px !important;
  border: 1px solid {SPOTIFY_BORDER} !important;
}}
[data-testid="stDataFrame"] {{
  border: 1px solid {SPOTIFY_BORDER};
  border-radius: 14px;
  overflow: hidden;
}}
.small-muted {{
  color: {SPOTIFY_MUTED};
  font-size: 12px;
}}
</style>
""",
    unsafe_allow_html=True,
)

# -----------------------------
# Session state
# -----------------------------
if "refresh_key" not in st.session_state:
    st.session_state["refresh_key"] = 0
if "render_dashboard" not in st.session_state:
    st.session_state["render_dashboard"] = False
if "active_sheet_id" not in st.session_state:
    st.session_state["active_sheet_id"] = None  # chosen & loaded sheet
if "inited_sheet_id" not in st.session_state:
    st.session_state["inited_sheet_id"] = None  # structure ensured for this sheet in this session
if "pending_auth_url" not in st.session_state:
    st.session_state["pending_auth_url"] = None
if "registry_cache" not in st.session_state:
    st.session_state["registry_cache"] = {"ts": None, "registered": False, "enabled": False, "existing_sheet": None}
if "sheet_input" not in st.session_state:
    st.session_state["sheet_input"] = ""
if "min_data_date" not in st.session_state:
    st.session_state["min_data_date"] = None  # earliest date in df_log, used for "All time"

# -----------------------------
# Helpers
# -----------------------------
def extract_sheet_id(text: str) -> str | None:
    text = (text or "").strip()
    if not text:
        return None
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", text)
    if m:
        return m.group(1)
    if re.fullmatch(r"[a-zA-Z0-9-_]{20,}", text):
        return text
    return None


def get_query_param(name: str) -> str | None:
    # Streamlit new API + fallback
    try:
        v = st.query_params.get(name)
        if isinstance(v, list):
            return v[0] if v else None
        return v
    except Exception:
        qp = st.experimental_get_query_params()
        arr = qp.get(name)
        return arr[0] if arr else None


def set_query_params(**kwargs: str) -> None:
    # Streamlit new API + fallback
    try:
        st.query_params.clear()
        for k, v in kwargs.items():
            st.query_params[k] = v
    except Exception:
        st.experimental_set_query_params(**kwargs)


def clear_query_params() -> None:
    try:
        st.query_params.clear()
    except Exception:
        st.experimental_set_query_params()


def redirect_same_tab(url: str) -> None:
    # Runs inside an iframe; use window.top to navigate the main tab
    safe = json.dumps(url)
    components.html(
        f"""
        <script>
          const url = {safe};
          try {{
            window.top.location.href = url;
          }} catch (e) {{
            try {{
              window.parent.location.href = url;
            }} catch (e2) {{
              window.location.href = url;
            }}
          }}
        </script>
        """,
        height=0,
        width=0,
    )


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _b64url_decode(s: str) -> bytes:
    s = (s or "").strip()
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("utf-8"))


def encode_oauth_state(*, sheet_id: str, now_utc: datetime, secret: str) -> str:
    """
    Signed OAuth state:
      state = b64url(payload_json) + "." + b64url(hmac_sha256(payload_json))
    No need to store oauth_state in Google Sheets.
    """
    payload = {
        "sid": sheet_id,
        "ts": int(now_utc.timestamp()),
        "n": _b64url(hashlib.sha256(f"{sheet_id}:{now_utc.timestamp()}".encode("utf-8")).digest())[:18],
    }
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).digest()
    return f"{_b64url(raw)}.{_b64url(sig)}"


def decode_oauth_state(state: str, *, secret: str, max_age_seconds: int = 3600) -> dict[str, Any] | None:
    try:
        s = (state or "").strip()
        if not s or "." not in s:
            return None
        p_b64, sig_b64 = s.split(".", 1)
        raw = _b64url_decode(p_b64)
        sig = _b64url_decode(sig_b64)

        expected = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).digest()
        if not hmac.compare_digest(sig, expected):
            return None

        obj = json.loads(raw.decode("utf-8"))
        if not isinstance(obj, dict):
            return None

        sid = str(obj.get("sid") or "").strip()
        ts = int(obj.get("ts") or 0)
        if not sid or ts <= 0:
            return None

        now_ts = int(datetime.now(timezone.utc).timestamp())
        if abs(now_ts - ts) > max_age_seconds:
            return None

        return obj
    except Exception:
        return None


def parse_played_at_to_utc(date_str: str) -> datetime | None:
    s = (date_str or "").strip()
    if not s:
        return None
    try:
        dt = dtparser.parse(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def kpi_card(label: str, value: str) -> None:
    st.markdown(
        f"""
<div class="spotify-card">
  <div class="kpi-label">{label}</div>
  <div class="kpi">{value}</div>
</div>
""",
        unsafe_allow_html=True,
    )


def get_service_account_email(settings) -> str | None:
    try:
        j = json.loads(settings.google_service_account_json)
        return j.get("client_email")
    except Exception:
        return None


# -----------------------------
# Registry helpers (lazy usage only)
# -----------------------------
def get_registry_ws_best_effort(*, sheets: SheetsClient, settings) -> Any | None:
    try:
        registry_ss = sheets.open_by_key(settings.registry_sheet_id)
        registry_ws = sheets.get_or_create_worksheet(registry_ss, REGISTRY_TAB, rows=2000, cols=20)
        ensure_registry_headers(registry_ws)
        return registry_ws
    except Exception:
        return None


def registry_get_sheet_status(registry_ws, user_sheet_id: str) -> tuple[bool, bool]:
    try:
        rows = gcall(lambda: registry_ws.get_all_values())
        for r in rows[1:]:
            sid = (r[0] or "").strip() if len(r) >= 1 else ""
            if sid == user_sheet_id:
                enabled_raw = (r[1] or "").strip().lower() if len(r) >= 2 else ""
                enabled = enabled_raw in ("true", "1", "yes", "y")
                return True, enabled
        return False, False
    except Exception:
        return False, False


# -----------------------------
# Cached reads (reduce 429 on reruns)
# -----------------------------
@st.cache_data(ttl=90, show_spinner=False)
def cached_ws_values(service_json: str, sheet_id: str, ws_title: str, refresh_key: int) -> list[list[str]]:
    sheets_local = SheetsClient.from_service_account_json(service_json)
    ss_local = sheets_local.open_by_key(sheet_id)
    ws = ss_local.worksheet(ws_title)
    return gcall(lambda: ws.get_all_values())


def df_from_ws_rows(rows: list[list[str]]) -> pd.DataFrame:
    if not rows or len(rows) < 2:
        return pd.DataFrame()
    header = rows[0]
    data = rows[1:]
    return pd.DataFrame(data, columns=header[: len(header)])


def load_log_df_cached(settings, sheet_id: str) -> pd.DataFrame:
    rows = cached_ws_values(settings.google_service_account_json, sheet_id, "log", st.session_state["refresh_key"])
    if not rows or len(rows) < 2:
        return pd.DataFrame(columns=["Date", "Track", "Artist", "Spotify ID", "URL"])

    header = rows[0]
    data = rows[1:]
    df = pd.DataFrame(data, columns=header[: len(header)])

    for col in ["Date", "Track", "Artist", "Spotify ID", "URL"]:
        if col not in df.columns:
            df[col] = ""
    df = df[["Date", "Track", "Artist", "Spotify ID", "URL"]].copy()

    df["played_at_utc"] = df["Date"].apply(parse_played_at_to_utc)
    df = df[df["played_at_utc"].notna()].copy()
    df["played_at_utc"] = pd.to_datetime(df["played_at_utc"], utc=True)
    df = df.sort_values("played_at_utc", ascending=False)
    return df


def load_cache_tracks_df(settings, sheet_id: str) -> pd.DataFrame:
    rows = cached_ws_values(
        settings.google_service_account_json, sheet_id, "__cache_tracks", st.session_state["refresh_key"]
    )
    df = df_from_ws_rows(rows)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "track_id",
                "track_name",
                "duration_ms",
                "album_id",
                "album_cover_url",
                "primary_artist_id",
                "artist_ids",
                "track_url",
                "fetched_at",
            ]
        )
    return df


def load_cache_artists_df(settings, sheet_id: str) -> pd.DataFrame:
    rows = cached_ws_values(
        settings.google_service_account_json, sheet_id, "__cache_artists", st.session_state["refresh_key"]
    )
    df = df_from_ws_rows(rows)
    if df.empty:
        return pd.DataFrame(
            columns=["artist_id", "artist_name", "artist_cover_url", "genres", "primary_genre", "fetched_at"]
        )
    return df


def load_cache_albums_df(settings, sheet_id: str) -> pd.DataFrame:
    rows = cached_ws_values(
        settings.google_service_account_json, sheet_id, "__cache_albums", st.session_state["refresh_key"]
    )
    df = df_from_ws_rows(rows)
    if df.empty:
        return pd.DataFrame(columns=["album_id", "album_name", "album_cover_url", "release_date", "fetched_at"])
    return df


def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(str(x).strip()))
    except Exception:
        return default


def render_top_cards(items: list[dict[str, Any]], *, cols: int = 5) -> None:
    if not items:
        st.info("No data for the selected range.")
        return
    grid = st.columns(cols)
    for i, it in enumerate(items[:cols]):
        with grid[i % cols]:
            cover = (it.get("cover") or "").strip()
            if cover:
                st.image(cover, width=COVER_W)
            st.markdown(f"**{it.get('title','')}**")
            if it.get("subtitle"):
                st.markdown(f'<div class="small-muted">{it["subtitle"]}</div>', unsafe_allow_html=True)
            for line in it.get("lines", []):
                st.markdown(f"<div>{line}</div>", unsafe_allow_html=True)

# -----------------------------
# X axis helper
# -----------------------------
def x_bucket(grain: str) -> alt.X:
    """
    Render bucket on X as ORDINAL (categorical) to prevent Vega from inserting extra time ticks ("12 PM").
    Format:
      - Week  -> Monday date of the week
      - Month -> MM-YYYY
    """
    if grain == "Month":
        fmt = "%m-%Y"
        angle = 0
    else:
        fmt = "%d.%m.%Y"   # Monday date
        angle = -45

    return alt.X(
        "bucket_dt:O",  # <-- critical: ordinal
        title=None,
        sort=alt.SortField("bucket_dt", order="ascending"),
        axis=alt.Axis(
            labelAngle=angle,
            labelOverlap="greedy",
            labelExpr=f"timeFormat(datum.value, '{fmt}')",
        ),
    )


def period_tooltip():
    return alt.Tooltip("bucket_dt:T", title="Period")


def _hex_with_alpha(hex_color: str, alpha: float) -> str:
    """Convert #RRGGBB to rgba(r,g,b,a) for Vega/Altair."""
    hex_color = hex_color.lstrip("#")
    r = int(hex_color[0:2], 16)
    g = int(hex_color[2:4], 16)
    b = int(hex_color[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def render_activity_grid(
    *,
    df_log: pd.DataFrame,
    tz,                   # ZoneInfo или timezone.utc
) -> None:
    """
    GitHub-like activity grid for the LAST 365 days (rolling year back from today in tz).
    Each cell = day. Columns = weeks (Mon-start), rows = day of week.
    Metric: Plays only.
    Legend moved to the bottom (not on the side).
    """

    st.markdown("### Activity (last 365 days)")

    if df_log is None or df_log.empty:
        st.info("No activity data yet.")
        return

    # --- base plays in local time
    base = df_log[["played_at_utc", "Spotify ID"]].copy()
    base = base[base["played_at_utc"].notna()].copy()
    base["track_id"] = base["Spotify ID"].astype(str).fillna("")
    base = base[base["track_id"].str.len() > 0].copy()

    # tz-aware local time
    try:
        base["played_local"] = base["played_at_utc"].dt.tz_convert(tz)
    except Exception:
        base["played_local"] = pd.to_datetime(base["played_at_utc"], utc=True).dt.tz_convert(tz)

    base["day"] = base["played_local"].dt.date

    # --- rolling window: last 365 days inclusive
    today_local = datetime.now(timezone.utc).astimezone(tz).date()
    start = today_local - timedelta(days=364)  # inclusive range length 365
    end = today_local

    base = base[(base["day"] >= start) & (base["day"] <= end)].copy()
    if base.empty:
        st.info("No activity data in the last 365 days.")
        return

    # Plays per day
    daily = base.groupby("day", dropna=False).size().reset_index(name="value")

    # Full day grid (ensure empty days exist)
    all_days = pd.DataFrame({"day": pd.date_range(start, end, freq="D").date})
    grid = all_days.merge(daily, on="day", how="left").fillna({"value": 0.0})

    # --- map day -> (week_index, weekday)
    start_monday = start - timedelta(days=start.weekday())  # Monday on/before start
    grid["week"] = grid["day"].apply(lambda d: (d - start_monday).days // 7).astype(int)
    grid["dow"] = pd.to_datetime(grid["day"]).dt.weekday.astype(int)  # Mon=0..Sun=6

    n_weeks = int(grid["week"].max()) + 1

    # --- bin into levels like GitHub: 0 + 4 bins
    vals = grid.loc[grid["value"] > 0, "value"].astype(float)
    if len(vals) >= 10:
        q1, q2, q3, q4 = vals.quantile([0.25, 0.50, 0.75, 0.90]).tolist()
        cuts = [0, q1, q2, q3, q4]
    else:
        cuts = [0, 1, 3, 6, 10]

    def to_level(v: float) -> int:
        if v <= 0:
            return 0
        if v <= cuts[1]:
            return 1
        if v <= cuts[2]:
            return 2
        if v <= cuts[3]:
            return 3
        return 4

    grid["level"] = grid["value"].apply(to_level).astype(int)

    # --- colors (Spotify-ish)
    def rgba(hex_color: str, a: float) -> tuple[float, float, float, float]:
        h = hex_color.lstrip("#")
        r = int(h[0:2], 16) / 255.0
        g = int(h[2:4], 16) / 255.0
        b = int(h[4:6], 16) / 255.0
        return (r, g, b, a)

    palette = {
        0: rgba(SPOTIFY_BORDER, 1.0),
        1: rgba(SPOTIFY_GREEN, 0.25),
        2: rgba(SPOTIFY_GREEN, 0.45),
        3: rgba(SPOTIFY_GREEN, 0.65),
        4: rgba(SPOTIFY_GREEN, 1.0),
    }

    # --- draw
    cell = 1.0
    gap = 0.18

    # width (weeks) + a little left padding for weekday labels
    width = n_weeks * (cell + gap) + 3.5
    width_in = max(12, width / 6.2)
    height_in = 2.9  # a bit taller to visually match the new legend row

    fig, ax = plt.subplots(figsize=(width_in, height_in))
    fig.patch.set_facecolor(SPOTIFY_BG)
    ax.set_facecolor(SPOTIFY_BG)

    # cells
    for _, r in grid.iterrows():
        x = r["week"] * (cell + gap)
        y = r["dow"] * (cell + gap)
        rect = Rectangle((x, y), cell, cell, linewidth=0, facecolor=palette[int(r["level"])])
        ax.add_patch(rect)

    # layout
    ax.set_xlim(-2.5, n_weeks * (cell + gap) + 0.8)
    ax.set_ylim(7 * (cell + gap) + 1.6, -2.0)  # invert y + extra space at bottom for legend
    ax.axis("off")

    # left labels (Mon/Wed/Fri)
    label_map = {0: "Mon", 2: "Wed", 4: "Fri"}
    for dow, txt in label_map.items():
        y = dow * (cell + gap) + cell * 0.75
        ax.text(-2.1, y, txt, color=SPOTIFY_MUTED, fontsize=10, va="center")

    # month labels based on rolling window
    month_starts = pd.date_range(start, end, freq="MS").date
    used = set()
    for m in month_starts:
        w = (m - start_monday).days // 7
        if w < 0:
            continue
        if w in used:
            continue
        used.add(w)
        x = w * (cell + gap)
        ax.text(x, -0.95, m.strftime("%b"), color=SPOTIFY_MUTED, fontsize=10, ha="left", va="center")

    # legend moved to bottom (center-ish)
    # place it under the grid area
    legend_y = 7 * (cell + gap) + 0.25
    legend_x0 = max(0.0, (n_weeks * (cell + gap) - (5 * (cell + gap) + 4.0)) / 2.0)

    ax.text(legend_x0 - 1.1, legend_y + 0.55, "Less", color=SPOTIFY_MUTED, fontsize=10, va="center")
    for i in range(5):
        rect = Rectangle(
            (legend_x0 + i * (cell + gap), legend_y),
            cell,
            cell,
            linewidth=0,
            facecolor=palette[i],
        )
        ax.add_patch(rect)
    ax.text(
        legend_x0 + 5 * (cell + gap) + 0.3,
        legend_y + 0.55,
        "More",
        color=SPOTIFY_MUTED,
        fontsize=10,
        va="center",
    )

    st.pyplot(fig, use_container_width=True)
    plt.close(fig)


# -----------------------------
# Header
# -----------------------------
st.markdown(
    """
<div style="display:flex; align-items:center; gap:12px;">
  <div style="font-size:40px;">🎧</div>
  <div>
    <div style="font-size:44px; font-weight:900; line-height:1;">Spotify Logger</div>
    <div style="color:#B3B3B3; margin-top:6px;">Setup & Spotify connect + Background sync + Dashboard</div>
  </div>
</div>
""",
    unsafe_allow_html=True,
)

# -----------------------------
# Config + service account email
# -----------------------------
settings = load_settings()
sheets = SheetsClient.from_service_account_json(settings.google_service_account_json)
service_email = get_service_account_email(settings)

with st.expander("How to prepare Google Sheet", expanded=False):
    st.write("1) Create a Google Sheet (or use an existing one).")
    if service_email:
        st.write("2) Click **Share** → add this email as **Editor**:")
        st.code(service_email)
    else:
        st.warning("Could not read client_email from service account JSON. Check GOOGLE_SERVICE_ACCOUNT_JSON.")
    st.write("3) Paste the sheet link/ID below, then click **Load sheet**.")
    st.write("4) Connect Spotify (OAuth).")
    st.write("5) Enable background sync (optional).")

st.divider()

# -----------------------------
# OAuth callback handling (first thing: handle code/state before doing extra reads)
# -----------------------------
redirect_uri = settings.public_app_url.rstrip("/")
code = get_query_param("code")
state_cb = get_query_param("state")
error_cb = get_query_param("error")

if error_cb:
    st.error(f"Spotify auth error: {error_cb}")
    clear_query_params()
    st.stop()

# If Spotify redirected back with ?code&state — finish auth here.
# We do NOT need to read/write oauth_state in Sheets.
if code and state_cb:
    decoded = decode_oauth_state(state_cb, secret=settings.spotify_client_secret, max_age_seconds=3600)
    if not decoded:
        st.error("Invalid or expired OAuth state. Please click Connect Spotify again.")
        clear_query_params()
        st.stop()

    sid_from_state = str(decoded["sid"]).strip()

    # Open & init target sheet (one-time per session)
    try:
        ss_cb = sheets.open_by_key(sid_from_state)
        if st.session_state.get("inited_sheet_id") != sid_from_state:
            ensure_user_sheet_initialized(ss_cb, timezone_name="UTC")
            st.session_state["inited_sheet_id"] = sid_from_state
    except Exception as e:
        st.error("OAuth callback: cannot open/initialize the sheet from state.")
        st.write("Reason:", str(e))
        clear_query_params()
        st.stop()

    with st.spinner("Connecting Spotify... (exchange code → token)"):
        tokens = exchange_code_for_token(
            settings.spotify_client_id,
            settings.spotify_client_secret,
            redirect_uri,
            code,
        )

        if not tokens.refresh_token:
            st.error("Spotify didn't return a refresh_token. Please click Connect again.")
            clear_query_params()
            st.stop()

        spotify_user_id_cb = get_spotify_user_id(tokens.access_token)
        refresh_enc = encrypt_str(tokens.refresh_token, settings.fernet_key)

        write_app_state_kv(
            ss_cb,
            {
                "spotify_user_id": spotify_user_id_cb,
                "refresh_token_enc": refresh_enc,
                "last_error": "",
            },
        )

    # Return to app with sheet prefilled (no code/state in URL)
    clear_query_params()
    set_query_params(sheet=sid_from_state)

    # Also set the active sheet in this session so user sees UI immediately
    st.session_state["active_sheet_id"] = sid_from_state
    st.session_state["render_dashboard"] = False
    st.session_state["refresh_key"] += 1
    try:
        st.cache_data.clear()
    except Exception:
        pass

    st.success("Spotify connected! Returning to your sheet…")
    st.rerun()

# -----------------------------
# Sheet input (supports ?sheet=... auto-fill)
# -----------------------------
sheet_from_qp = get_query_param("sheet")
if sheet_from_qp and not st.session_state.get("sheet_input"):
    st.session_state["sheet_input"] = sheet_from_qp

sheet_input = st.text_input(
    "Paste your Google Sheet URL or Sheet ID",
    key="sheet_input",
    placeholder="https://docs.google.com/spreadsheets/d/<ID>/edit ...",
)

candidate_sheet_id = extract_sheet_id(sheet_input)

# "Load sheet" gating: we do not open Google Sheet on every rerun.
col_a, col_b = st.columns([1.2, 3])
with col_a:
    load_clicked = st.button("📄 Load sheet", width="stretch")
with col_b:
    st.markdown('<div class="small-muted">Click the button pls.</div>', unsafe_allow_html=True)

if load_clicked:
    if not candidate_sheet_id:
        st.warning("Paste a valid Google Sheet link/ID first.")
        st.stop()

    # Switching sheet: reset heavy stuff
    if st.session_state.get("active_sheet_id") != candidate_sheet_id:
        st.session_state["active_sheet_id"] = candidate_sheet_id
        st.session_state["render_dashboard"] = False
        st.session_state["refresh_key"] += 1
        st.session_state["registry_cache"] = {"ts": None, "registered": False, "enabled": False, "existing_sheet": None}
        st.session_state["min_data_date"] = None
        try:
            st.cache_data.clear()
        except Exception:
            pass

    set_query_params(sheet=candidate_sheet_id)
    st.rerun()

sheet_id = st.session_state.get("active_sheet_id")
if not sheet_id:
    st.info("Load a sheet to continue.")
    st.stop()

# -----------------------------
# Open user sheet (only after "Load sheet")
# -----------------------------
try:
    ss = sheets.open_by_key(sheet_id)
except Exception as e:
    st.error("❌ Can't open this Google Sheet with the service account.")
    st.write("Reason:", str(e))
    st.stop()

st.success("Google Sheet is accessible to the service account")

# Ensure structure exists (ONLY ONCE per session+sheet)
if st.session_state.get("inited_sheet_id") != sheet_id:
    try:
        ensure_user_sheet_initialized(ss, timezone_name="UTC")
        st.session_state["inited_sheet_id"] = sheet_id
    except Exception as e:
        st.error("❌ Failed to initialize the required worksheets (log, etc).")
        st.write("Reason:", str(e))
        st.stop()

st.success("Sheet structure OK (cached for this session)")

# -----------------------------
# Read minimal state (small read; unavoidable if you want status)
# -----------------------------
try:
    state = read_app_state(ss)
except gspread.exceptions.APIError as e:
    msg = str(e)
    if "429" in msg or "Quota" in msg or "quota" in msg:
        st.warning("Google Sheets quota exceeded (429). Wait ~60 seconds and reload.")
        st.stop()
    raise

enabled_local = (state.get("enabled") or "false").lower() == "true"
timezone_name = (state.get("timezone") or "UTC").strip() or "UTC"
spotify_connected = bool((state.get("refresh_token_enc") or "").strip())
spotify_user_id = (state.get("spotify_user_id") or "").strip()

# -----------------------------
# Status (registry is lazy: no reads unless user clicks "Check")
# -----------------------------
st.markdown("## Status")
c1, c2, c3, c4, c5, c6 = st.columns([1.05, 1.05, 1.05, 1.6, 1.35, 1.1])

with c1:
    st.markdown(
        f'<span class="badge">Local enabled</span> <span class="success-pill">true</span>'
        if enabled_local
        else f'<span class="badge">Local enabled</span> <span class="badge">false</span>',
        unsafe_allow_html=True,
    )
with c2:
    st.markdown(f'<span class="badge">Timezone</span> <span class="badge">{timezone_name}</span>', unsafe_allow_html=True)
with c3:
    st.markdown(
        f'<span class="badge">Spotify</span> <span class="success-pill">connected</span>'
        if spotify_connected
        else f'<span class="badge">Spotify</span> <span class="badge">not connected</span>',
        unsafe_allow_html=True,
    )
with c4:
    if spotify_user_id:
        st.markdown(
            f'<span class="badge">Spotify user id</span> <span class="badge">{spotify_user_id}</span>',
            unsafe_allow_html=True,
        )

# Registry status: cached for 60s if user checked
reg = st.session_state.get("registry_cache") or {}
reg_ts = reg.get("ts")
reg_fresh = reg_ts and (datetime.now(timezone.utc) - reg_ts) < timedelta(seconds=60)

with c5:
    if reg_fresh:
        bg_on = bool(reg.get("registered")) and bool(reg.get("enabled"))
        st.markdown(
            f'<span class="badge">Background sync</span> <span class="success-pill">ON</span>'
            if bg_on
            else f'<span class="badge">Background sync</span> <span class="badge">OFF</span>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown('<span class="badge">Background sync</span> <span class="badge">unknown</span>', unsafe_allow_html=True)

with c6:
    check_registry = st.button("Check", width="stretch")

if check_registry:
    registry_ws = get_registry_ws_best_effort(sheets=sheets, settings=settings)
    registered, enabled_registry = (False, False)
    existing_sheet_for_user: str | None = None

    if registry_ws is not None:
        registered, enabled_registry = registry_get_sheet_status(registry_ws, sheet_id)
        if spotify_connected and spotify_user_id:
            try:
                existing_sheet_for_user = find_sheet_by_spotify_user_id(registry_ws, spotify_user_id)
            except Exception:
                existing_sheet_for_user = None

    st.session_state["registry_cache"] = {
        "ts": datetime.now(timezone.utc),
        "registered": registered,
        "enabled": enabled_registry,
        "existing_sheet": existing_sheet_for_user,
    }
    st.rerun()

existing_sheet_for_user = (st.session_state.get("registry_cache") or {}).get("existing_sheet")
background_sync_on = False
if reg_fresh:
    background_sync_on = bool(reg.get("registered")) and bool(reg.get("enabled"))

if spotify_connected and spotify_user_id and existing_sheet_for_user and existing_sheet_for_user != sheet_id:
    st.warning(
        "⚠️ This Spotify account is already syncing in another sheet.\n\n"
        f"**Active sheet:** `{existing_sheet_for_user}`\n\n"
        "Background sync cannot be enabled here (limitation: 1 sheet per 1 Spotify account)."
    )

# -----------------------------
# Actions
# -----------------------------
st.markdown("## Actions")

# If we generated an auth url on previous run — redirect now
if st.session_state.get("pending_auth_url"):
    url = st.session_state.pop("pending_auth_url")
    redirect_same_tab(url)
    st.link_button("If you are not redirected, click here", url)
    st.stop()

if not spotify_connected:
    st.warning("Spotify is not connected yet. Connect via OAuth.")

    if st.button("Connect Spotify"):
        # Build signed state (NO Google write)
        oauth_state = encode_oauth_state(
            sheet_id=sheet_id,
            now_utc=datetime.now(timezone.utc),
            secret=settings.spotify_client_secret,
        )

        scopes = ["user-read-recently-played", "user-read-email", "user-read-private"]
        url = build_auth_url(
            client_id=settings.spotify_client_id,
            redirect_uri=redirect_uri,
            scopes=scopes,
            state=oauth_state,
        )

        # redirect on next run (reliable on Streamlit Cloud)
        st.session_state["pending_auth_url"] = url
        st.rerun()

else:
    st.success("Spotify is connected ✅")

    col1, col2, col3, col4 = st.columns([1.4, 1.4, 1.3, 1.9])

    # Enable background sync
    with col1:
        if not background_sync_on:
            if st.button("Enable background sync"):
                registry_ws = get_registry_ws_best_effort(sheets=sheets, settings=settings)
                if registry_ws is None:
                    st.error("Registry sheet is not accessible to the service account. Cron cannot work.")
                    st.stop()
                if not spotify_user_id:
                    st.error("spotify_user_id is missing in __app_state. Reconnect Spotify.")
                    st.stop()

                existing = None
                try:
                    existing = find_sheet_by_spotify_user_id(registry_ws, spotify_user_id)
                except Exception:
                    existing = None

                if existing and existing != sheet_id:
                    st.error(
                        "This Spotify account is already connected to another sheet.\n\n"
                        f"**Active sheet:** `{existing}`\n\n"
                        "Background sync will not be enabled here."
                    )
                    st.stop()

                try:
                    upsert_registry_user(
                        registry_ws,
                        user_sheet_id=sheet_id,
                        enabled=True,
                        spotify_user_id=spotify_user_id,
                    )
                except TypeError:
                    upsert_registry_user(registry_ws, user_sheet_id=sheet_id, enabled=True)

                write_app_state_kv(ss, {"enabled": "true"})
                st.session_state["registry_cache"] = {"ts": None, "registered": False, "enabled": False, "existing_sheet": None}
                st.success("Background sync enabled")
                st.rerun()

    # Disable background sync
    with col2:
        if background_sync_on:
            if st.button("Disable background sync"):
                registry_ws = get_registry_ws_best_effort(sheets=sheets, settings=settings)
                if registry_ws is None:
                    st.error("Registry sheet is not accessible to the service account.")
                    st.stop()

                try:
                    upsert_registry_user(
                        registry_ws,
                        user_sheet_id=sheet_id,
                        enabled=False,
                        spotify_user_id=spotify_user_id or None,
                    )
                except TypeError:
                    upsert_registry_user(registry_ws, user_sheet_id=sheet_id, enabled=False)

                write_app_state_kv(ss, {"enabled": "false"})
                st.session_state["registry_cache"] = {"ts": None, "registered": False, "enabled": False, "existing_sheet": None}
                st.info("Background sync disabled")
                st.rerun()

    with col3:
        if st.button("Refresh data"):
            st.session_state["refresh_key"] += 1
            st.rerun()

    with col4:
        st.caption(
            "The dashboard reads Google Sheets data via cache.\n"
            "If you hit 429, wait ~60 seconds and click Refresh data."
        )

st.divider()

# -----------------------------
# Dashboard controls
# -----------------------------
st.markdown("## Dashboard")

with st.sidebar:
    st.markdown("### Time range")

    presets = ["This year", "All time", "Last 7 days", "Last 30 days", "Last 90 days", "Custom"]
    preset = st.selectbox("Quick range", presets, index=0)

    today_utc = datetime.now(timezone.utc).date()
    current_year = today_utc.year

    min_data_date = st.session_state.get("min_data_date")  # may be None until we load df_log once

    if preset == "This year":
        default_from = datetime(current_year, 1, 1, tzinfo=timezone.utc).date()
        default_to = today_utc

    elif preset == "All time":
        default_from = min_data_date or datetime(current_year, 1, 1, tzinfo=timezone.utc).date()
        default_to = today_utc

    elif preset == "Last 7 days":
        default_from = (datetime.now(timezone.utc) - timedelta(days=7)).date()
        default_to = today_utc

    elif preset == "Last 30 days":
        default_from = (datetime.now(timezone.utc) - timedelta(days=30)).date()
        default_to = today_utc

    elif preset == "Last 90 days":
        default_from = (datetime.now(timezone.utc) - timedelta(days=90)).date()
        default_to = today_utc

    else:  # Custom
        default_from = (datetime.now(timezone.utc) - timedelta(days=30)).date()
        default_to = today_utc

    picked = st.date_input("From / To", value=(default_from, default_to))
    if isinstance(picked, tuple) and len(picked) == 2:
        date_from, date_to = picked
    else:
        date_from = picked
        date_to = picked

    st.markdown('<div class="small-muted">Rendering happens only on button click.</div>', unsafe_allow_html=True)
    if st.button("▶ Render dashboard", width="stretch"):
        st.session_state["render_dashboard"] = True
        st.session_state["refresh_key"] += 1
        st.rerun()

if not st.session_state.get("render_dashboard"):
    st.info("Pick a date range in the sidebar and click **Render dashboard**.")
    st.stop()

# -----------------------------
# Load data (log + caches)
# -----------------------------
try:
    df_log = load_log_df_cached(settings, sheet_id)

    # Store min date for "All time" preset (once we actually have data)
    try:
        if df_log is not None and len(df_log) > 0:
            st.session_state["min_data_date"] = df_log["played_at_utc"].min().date()
        else:
            st.session_state["min_data_date"] = None
    except Exception:
        st.session_state["min_data_date"] = None

    # Prepare full-history mapping for New vs Repeat (do NOT depend on selected range)
    df_hist = df_log.rename(columns={"Spotify ID": "track_id"}).copy()
    df_hist["track_id"] = df_hist["track_id"].astype(str)
    df_hist = df_hist[df_hist["track_id"].str.len() > 0].copy()
    first_play = (
        df_hist.groupby("track_id", dropna=False)["played_at_utc"]
        .min()
        .reset_index(name="first_play_utc")
    )

    df_ct = load_cache_tracks_df(settings, sheet_id)
    df_ca = load_cache_artists_df(settings, sheet_id)
    df_calb = load_cache_albums_df(settings, sheet_id)
except gspread.exceptions.APIError as e:
    msg = str(e)
    if "Quota exceeded" in msg or "[429]" in msg or "429" in msg:
        st.warning("Google Sheets quota exceeded (429). Wait ~60 seconds and click **Refresh data**.")
        st.stop()
    raise

if df_log.empty:
    st.info("No rows in log yet. Wait until the worker appends some plays.")
    st.stop()

# Filter by date range in user's timezone
try:
    tz = ZoneInfo(timezone_name or "UTC")
except Exception:
    tz = timezone.utc

start_dt = datetime.combine(date_from, time.min).replace(tzinfo=tz).astimezone(timezone.utc)
end_dt = datetime.combine(date_to, time.max).replace(tzinfo=tz).astimezone(timezone.utc)

df = df_log.copy()
df = df[(df["played_at_utc"] >= pd.Timestamp(start_dt)) & (df["played_at_utc"] <= pd.Timestamp(end_dt))].copy()

if df.empty:
    st.info("No data in the selected range.")
    st.stop()

# Normalize cache columns
df_ct = df_ct.copy()
df_ct["duration_ms_i"] = df_ct.get("duration_ms", "").apply(lambda x: safe_int(x, 0))
df_ct["album_id"] = df_ct.get("album_id", "").astype(str)
df_ct["primary_artist_id"] = df_ct.get("primary_artist_id", "").astype(str)

df_calb = df_calb.copy()
df_calb["album_id"] = df_calb.get("album_id", "").astype(str)

df_ca = df_ca.copy()
df_ca["artist_id"] = df_ca.get("artist_id", "").astype(str)

# Enrich plays with cache info
df = df.rename(columns={"Spotify ID": "track_id"}).copy()
df["track_id"] = df["track_id"].astype(str)

df = df.merge(
    df_ct[["track_id", "duration_ms_i", "album_id", "album_cover_url", "primary_artist_id", "track_name"]],
    on="track_id",
    how="left",
)
df = df.merge(
    df_calb[["album_id", "album_name", "album_cover_url"]].rename(columns={"album_cover_url": "album_cover_url_from_albums"}),
    on="album_id",
    how="left",
)
df = df.merge(
    df_ca[["artist_id", "artist_name", "artist_cover_url", "genres", "primary_genre"]].rename(columns={"artist_id": "primary_artist_id"}),
    on="primary_artist_id",
    how="left",
)

# Pick best covers
df["track_cover_url"] = df["album_cover_url"].fillna("")
df["album_cover_best"] = df["album_cover_url_from_albums"].fillna("")
df["artist_cover_best"] = df["artist_cover_url"].fillna("")

# Minutes listened
df["minutes"] = (df["duration_ms_i"] / 60000.0).fillna(0.0)

# -----------------------------
# KPIs (selected period)
# -----------------------------
k1, k2, k3, k4, k5 = st.columns([1, 1, 1, 1, 1.2])
with k1:
    kpi_card("Total plays", str(len(df)))
with k2:
    kpi_card("Unique tracks", str(df["track_id"].nunique()))
with k3:
    kpi_card("Unique artists", str(df["Artist"].nunique()))
with k4:
    kpi_card("Minutes listened", str(int(round(df["minutes"].sum(), 0))))
with k5:
    active_days_sel = df["played_at_utc"].dt.date.nunique()
    kpi_card("Active days", str(active_days_sel))

# Activity grid

render_activity_grid(
    df_log=df_log,
    tz=tz,
)

st.divider()

# -----------------------------
# Tabs
# -----------------------------
tab_artists, tab_tracks, tab_albums, tab_monthly, tab_genres, tab_fingerprint, tab_new_repeat = st.tabs(
    ["Top 5 Artists", "Top 5 Tracks", "Top 5 Albums", "Monthly avg", "Top 5 Genres", "Listening fingerprint", "New vs Repeat"]
)

# ===== Top 5 Artists =====
with tab_artists:
    g = (
        df.groupby(["Artist"], dropna=False)
        .agg(plays=("track_id", "count"), minutes=("minutes", "sum"))
        .reset_index()
        .sort_values(["plays", "minutes"], ascending=False)
        .head(5)
    )

    cover_map = (
        df.dropna(subset=["Artist"])
        .groupby("Artist")["artist_cover_best"]
        .agg(lambda s: next((x for x in s if isinstance(x, str) and x.strip()), ""))
        .to_dict()
    )

    items = []
    for _, r in g.iterrows():
        name = str(r["Artist"])
        items.append(
            {
                "cover": cover_map.get(name, ""),
                "title": name,
                "subtitle": "",
                "lines": [
                    f"<span class='small-muted'>Listened tracks:</span> <b>{int(r['plays'])}</b>",
                    f"<span class='small-muted'>Listened minutes:</span> <b>{int(round(r['minutes'],0))}</b>",
                ],
            }
        )
    render_top_cards(items, cols=5)

# ===== Top 5 Tracks =====
with tab_tracks:
    g = (
        df.groupby(["Track", "Artist", "track_id"], dropna=False)
        .agg(plays=("track_id", "count"), minutes=("minutes", "sum"))
        .reset_index()
        .sort_values(["plays", "minutes"], ascending=False)
        .head(5)
    )

    cover_map = (
        df.groupby("track_id")["track_cover_url"]
        .agg(lambda s: next((x for x in s if isinstance(x, str) and x.strip()), ""))
        .to_dict()
    )

    items = []
    for _, r in g.iterrows():
        tid = str(r["track_id"])
        items.append(
            {
                "cover": cover_map.get(tid, ""),
                "title": str(r["Track"]),
                "subtitle": str(r["Artist"]),
                "lines": [
                    f"<span class='small-muted'>Times listened:</span> <b>{int(r['plays'])}</b>",
                    f"<span class='small-muted'>Minutes listened:</span> <b>{int(round(r['minutes'],0))}</b>",
                ],
            }
        )
    render_top_cards(items, cols=5)

# ===== Top 5 Albums =====
with tab_albums:
    g = (
        df.groupby(["album_id", "album_name"], dropna=False)
        .agg(plays=("track_id", "count"), minutes=("minutes", "sum"))
        .reset_index()
        .sort_values(["plays", "minutes"], ascending=False)
        .head(5)
    )

    cover_map = (
        df.groupby("album_id")["album_cover_best"]
        .agg(lambda s: next((x for x in s if isinstance(x, str) and x.strip()), ""))
        .to_dict()
    )

    items = []
    for _, r in g.iterrows():
        aid = str(r["album_id"])
        name = str(r["album_name"]).strip() or "(Unknown album)"
        items.append(
            {
                "cover": cover_map.get(aid, ""),
                "title": name,
                "subtitle": "",
                "lines": [
                    f"<span class='small-muted'>Times listened:</span> <b>{int(r['plays'])}</b>",
                    f"<span class='small-muted'>Minutes listened:</span> <b>{int(round(r['minutes'],0))}</b>",
                ],
            }
        )
    render_top_cards(items, cols=5)

# ===== Monthly avg (avg per active day) + cover markers on the line =====
with tab_monthly:
    st.markdown("### Average plays per active day by month")

    dfm = df.copy()
    dfm["month"] = dfm["played_at_utc"].dt.to_period("M").dt.to_timestamp()

    active_days = (
        dfm.groupby("month")["played_at_utc"]
        .apply(lambda s: s.dt.date.nunique())
        .reset_index(name="active_days")
    )

    month_agg = (
        dfm.groupby("month")
        .agg(plays=("track_id", "count"), minutes=("minutes", "sum"))
        .reset_index()
        .sort_values("month")
    )

    month_agg = month_agg.merge(active_days, on="month", how="left")
    month_agg["active_days"] = month_agg["active_days"].fillna(0).astype(int)

    month_agg["avg_tracks_per_active_day"] = month_agg.apply(
        lambda r: (r["plays"] / r["active_days"]) if r["active_days"] > 0 else 0.0,
        axis=1,
    )
    month_agg["avg_minutes_per_active_day"] = month_agg.apply(
        lambda r: (r["minutes"] / r["active_days"]) if r["active_days"] > 0 else 0.0,
        axis=1,
    )

    top_album = (
        dfm.groupby(["month", "album_id", "album_name"])
        .size()
        .reset_index(name="plays_album")
        .sort_values(["month", "plays_album"], ascending=[True, False])
    )
    top_album = top_album.groupby("month").head(1)

    cover_by_album = (
        dfm.groupby("album_id")["album_cover_best"]
        .agg(lambda s: next((x for x in s if isinstance(x, str) and x.strip()), ""))
        .to_dict()
    )
    top_album["album_cover_url"] = top_album["album_id"].map(cover_by_album)

    plays_m = month_agg.merge(top_album[["month", "album_name", "album_cover_url"]], on="month", how="left")
    plays_m["month_str"] = plays_m["month"].dt.strftime("%Y-%m")

    tooltip_main = [
        alt.Tooltip("month_str:N", title="Month"),
        alt.Tooltip("avg_tracks_per_active_day:Q", title="Avg tracks / active day", format=".2f"),
        alt.Tooltip("avg_minutes_per_active_day:Q", title="Avg minutes / active day", format=".1f"),
        alt.Tooltip("plays:Q", title="Tracks played (total)", format=",d"),
        alt.Tooltip("active_days:Q", title="Active days", format=",d"),
    ]

    base = alt.Chart(plays_m).encode(
        x=alt.X("month_str:N", title=None),
    )

    line = base.mark_line(color=SPOTIFY_GREEN).encode(
        y=alt.Y("avg_tracks_per_active_day:Q", title="Avg tracks / active day"),
        tooltip=tooltip_main,
    )

    points = base.mark_point(color=SPOTIFY_GREEN, size=70).encode(
        y="avg_tracks_per_active_day:Q",
        tooltip=tooltip_main,
    )

    img_df = plays_m[plays_m["album_cover_url"].fillna("").astype(str).str.len() > 0].copy()

    tooltip_cover = [
        alt.Tooltip("month_str:N", title="Month"),
        alt.Tooltip("album_name:N", title="Top album"),
        alt.Tooltip("avg_tracks_per_active_day:Q", title="Avg tracks / active day", format=".2f"),
        alt.Tooltip("plays:Q", title="Tracks played (total)", format=",d"),
        alt.Tooltip("active_days:Q", title="Active days", format=",d"),
    ]

    covers = alt.Chart(img_df).mark_image(width=42, height=42, dy=-32).encode(
        x="month_str:N",
        y="avg_tracks_per_active_day:Q",
        url="album_cover_url:N",
        tooltip=tooltip_cover,
    )

    st.altair_chart((line + points + covers).properties(height=500), width="stretch")

# ===== Top 5 Genres =====
with tab_genres:
    gen = df.copy()
    gen["primary_genre"] = gen.get("primary_genre", "").fillna("").astype(str).str.strip()
    gen = gen[gen["primary_genre"].str.len() > 0].copy()

    if gen.empty:
        st.info("Genres are empty (artist cache may not be filled yet). Run cache enrichment/backfill.")
    else:
        g = (
            gen.groupby("primary_genre")
            .size()
            .reset_index(name="plays")
            .sort_values("plays", ascending=False)
            .head(5)
        )

        ch = (
            alt.Chart(g)
            .mark_bar(color=SPOTIFY_GREEN)
            .encode(
                x=alt.X("plays:Q", title="Tracks"),
                y=alt.Y("primary_genre:N", sort="-x", title=None),
                tooltip=[
                    alt.Tooltip("primary_genre:N", title="Genre"),
                    alt.Tooltip("plays:Q", title="Plays", format=",d"),
                ],
            )
            .properties(height=300)
        )
        st.altair_chart(ch, width="stretch")

# ===== Listening fingerprint (day of week × hour heatmap) =====
with tab_fingerprint:
    st.markdown("### Listening fingerprint")

    # --- Timezone presets for this chart (doesn't change app_state timezone)
    tz_presets = {
        "Use sheet timezone": None,                 # то, что уже в app_state (timezone_name)
        "London": "Europe/London",
        "Belgrade": "Europe/Belgrade",
        "Moscow": "Europe/Moscow",
        "Amsterdam": "Europe/Amsterdam",
        "UTC": "UTC",
    }

    # дефолт: "Use sheet timezone"
    tz_label = st.selectbox(
        "Timezone preset",
        list(tz_presets.keys()),
        index=0,
        key="fingerprint_tz_preset",
    )

    # resolve timezone used in fingerprint
    tz_override = tz_presets[tz_label]
    try:
        tz_fp = ZoneInfo(tz_override) if tz_override else tz  # tz уже посчитан выше по timezone_name
    except Exception:
        tz_fp = tz  # fallback

    st.markdown(
        f'<div class="small-muted">Day of week × hour ({tz_fp.key})</div>',
        unsafe_allow_html=True,
    )

    metric = st.radio(
        "Metric",
        ["Plays", "Minutes"],
        horizontal=True,
        index=0,
        key="fingerprint_metric",
    )

    dff = df.copy()
    dff["played_local"] = dff["played_at_utc"].dt.tz_convert(tz_fp)
    dff["hour"] = dff["played_local"].dt.hour.astype(int)
    dff["dow"] = dff["played_local"].dt.day_name()

    dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    agg = (
        dff.groupby(["dow", "hour"], dropna=False)
        .agg(plays=("track_id", "count"), minutes=("minutes", "sum"))
        .reset_index()
    )

    if agg.empty:
        st.info("Not enough data for the selected range.")
    else:
        value_col = "plays" if metric == "Plays" else "minutes"
        value_title = "Plays" if metric == "Plays" else "Minutes"

        tooltip_fp = [
            alt.Tooltip("dow:N", title="Day"),
            alt.Tooltip("hour:Q", title="Hour"),
            alt.Tooltip(f"{value_col}:Q", title=value_title, format=".0f" if metric == "Plays" else ".1f"),
        ]

        color_scale = alt.Scale(range=[SPOTIFY_BG, SPOTIFY_GREEN])

        ch = (
            alt.Chart(agg)
            .mark_rect(cornerRadius=4)
            .encode(
                x=alt.X("hour:O", title="Hour", axis=alt.Axis(labelAngle=0)),
                y=alt.Y("dow:N", title=None, sort=dow_order),
                color=alt.Color(f"{value_col}:Q", title=value_title, scale=color_scale),
                tooltip=tooltip_fp,
            )
            .properties(height=320, background=SPOTIFY_BG)
            .configure_view(strokeOpacity=0)
            .configure_axis(
                labelColor=SPOTIFY_MUTED,
                titleColor=SPOTIFY_MUTED,
                gridColor=SPOTIFY_BORDER,
                tickColor=SPOTIFY_BORDER,
                domainColor=SPOTIFY_BORDER,
            )
            .configure_legend(labelColor=SPOTIFY_MUTED, titleColor=SPOTIFY_MUTED)
        )

        st.altair_chart(ch, width="stretch")

# ===== New vs Repeat =====
with tab_new_repeat:
    st.markdown("### New vs Repeat")
    st.markdown(
        '<div class="small-muted">New = first time a track appears in your whole log. Repeat = all other plays.</div>',
        unsafe_allow_html=True,
    )

    col_a, col_b = st.columns([1, 1.4])
    with col_a:
        grain = st.selectbox("Granularity", ["Week", "Month"], index=0, key="new_repeat_grain")
    with col_b:
        mode = st.selectbox("Metric", ["Plays", "Minutes", "Unique tracks"], index=0, key="new_repeat_mode")

    dnr = df[["track_id", "played_at_utc", "minutes"]].copy()
    dnr = dnr.merge(first_play, on="track_id", how="left")

    # Bucket in LOCAL time (naive for to_period)
    played_local_naive = dnr["played_at_utc"].dt.tz_convert(tz).dt.tz_localize(None)
    first_local_naive = dnr["first_play_utc"].dt.tz_convert(tz).dt.tz_localize(None)

    if grain == "Month":
        dnr["bucket"] = played_local_naive.dt.to_period("M").dt.to_timestamp()
        dnr["first_bucket"] = first_local_naive.dt.to_period("M").dt.to_timestamp()
    else:
        # week starting Monday
        dnr["bucket"] = played_local_naive.dt.to_period("W-MON").dt.start_time
        dnr["first_bucket"] = first_local_naive.dt.to_period("W-MON").dt.start_time

    dnr["is_new"] = dnr["bucket"] == dnr["first_bucket"]
    dnr["type"] = dnr["is_new"].map({True: "New", False: "Repeat"})

    # -----------------------
    # 1) PLAYS: stacked bars + New share line (%)
    # -----------------------
    if mode == "Plays":
        agg_wide = (
            dnr.groupby(["bucket", "type"], dropna=False)
            .size()
            .reset_index(name="plays")
            .pivot_table(index="bucket", columns="type", values="plays", fill_value=0)
            .reset_index()
        )

        # Ensure both columns exist
        if "New" not in agg_wide.columns:
            agg_wide["New"] = 0
        if "Repeat" not in agg_wide.columns:
            agg_wide["Repeat"] = 0

        agg_wide["total"] = agg_wide["New"] + agg_wide["Repeat"]
        agg_wide["new_share"] = agg_wide.apply(
            lambda r: (r["New"] / r["total"]) if r["total"] > 0 else 0.0,
            axis=1,
        )

        # ---- Critical: bucket_dt for X axis (ordinal)
        agg_wide["bucket_dt"] = pd.to_datetime(agg_wide["bucket"]).dt.normalize()

        # Exploration score (weighted by plays) = total_new / total_plays
        total_all = float(agg_wide["total"].sum())
        exploration_score = (float(agg_wide["New"].sum()) / total_all) if total_all > 0 else 0.0

        st.markdown(
            f"""
    <div class="spotify-card" style="margin-bottom:12px;">
      <div class="kpi-label">Exploration score (New share, weighted by plays)</div>
      <div class="kpi">{exploration_score * 100:.1f}%</div>
      <div class="small-muted">Higher = you spend more time discovering new tracks.</div>
    </div>
    """,
            unsafe_allow_html=True,
        )

        # long for stacked bars
        bars_df = agg_wide.melt(
            id_vars=["bucket", "bucket_dt", "total", "new_share"],
            value_vars=["New", "Repeat"],
            var_name="type",
            value_name="value",
        )

        color_scale = alt.Scale(domain=["New", "Repeat"], range=[SPOTIFY_GREEN, SPOTIFY_BORDER])

        # --- Top chart: New share line + points
        line_df = agg_wide[["bucket_dt", "new_share", "New", "Repeat", "total"]].copy()

        share_line = (
            alt.Chart(line_df)
            .mark_line(color=SPOTIFY_GREEN, strokeWidth=2.5)
            .encode(
                x=x_bucket(grain),
                y=alt.Y(
                    "new_share:Q",
                    title="New share",
                    scale=alt.Scale(domain=[0, 1]),
                    axis=alt.Axis(format="%"),
                ),
                tooltip=[
                    period_tooltip(),
                    alt.Tooltip("new_share:Q", title="New share", format=".1%"),
                    alt.Tooltip("New:Q", title="New plays", format=",d"),
                    alt.Tooltip("Repeat:Q", title="Repeat plays", format=",d"),
                    alt.Tooltip("total:Q", title="Total plays", format=",d"),
                ],
            )
        )

        share_points = (
            alt.Chart(line_df)
            .mark_point(color=SPOTIFY_GREEN, size=75, filled=True)
            .encode(
                x=x_bucket(grain),
                y=alt.Y("new_share:Q", scale=alt.Scale(domain=[0, 1]), axis=alt.Axis(format="%")),
                tooltip=[
                    period_tooltip(),
                    alt.Tooltip("new_share:Q", title="New share", format=".1%"),
                ],
            )
        )

        share_chart = (share_line + share_points).properties(height=150)

        # --- Bottom chart: stacked bars (plays)
        bars = (
            alt.Chart(bars_df)
            .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
            .encode(
                x=x_bucket(grain),
                y=alt.Y("value:Q", title="Plays", stack=True),
                color=alt.Color("type:N", title=None, scale=color_scale),
                tooltip=[
                    period_tooltip(),
                    alt.Tooltip("type:N", title="Type"),
                    alt.Tooltip("value:Q", title="Plays", format=",d"),
                    alt.Tooltip("total:Q", title="Total plays", format=",d"),
                    alt.Tooltip("new_share:Q", title="New share", format=".1%"),
                ],
            )
            .properties(height=260)
        )

        combo = (
            alt.vconcat(share_chart, bars, spacing=6)
            .resolve_scale(x="shared")
            .configure_view(fill=SPOTIFY_BG, strokeOpacity=0)
            .configure_axis(
                labelColor=SPOTIFY_MUTED,
                titleColor=SPOTIFY_MUTED,
                gridColor=SPOTIFY_BORDER,
                tickColor=SPOTIFY_BORDER,
                domainColor=SPOTIFY_BORDER,
            )
            .configure_legend(labelColor=SPOTIFY_MUTED, titleColor=SPOTIFY_MUTED)
        )

        st.altair_chart(combo, width="stretch")

    # -----------------------
    # 2) MINUTES: stacked bars
    # -----------------------
    elif mode == "Minutes":
        agg = (
            dnr.groupby(["bucket", "type"], dropna=False)
            .agg(value=("minutes", "sum"))
            .reset_index()
        )

        if agg.empty:
            st.info("Not enough data for the selected range.")
        else:
            agg["bucket_dt"] = pd.to_datetime(agg["bucket"]).dt.normalize()

            color_scale = alt.Scale(domain=["New", "Repeat"], range=[SPOTIFY_GREEN, SPOTIFY_BORDER])
            tooltip_nr = [
                period_tooltip(),
                alt.Tooltip("type:N", title="Type"),
                alt.Tooltip("value:Q", title="Minutes", format=".1f"),
            ]

            ch = (
                alt.Chart(agg)
                .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                .encode(
                    x=x_bucket(grain),
                    y=alt.Y("value:Q", title="Minutes", stack=True),
                    color=alt.Color("type:N", title=None, scale=color_scale),
                    tooltip=tooltip_nr,
                )
                .properties(height=360, background=SPOTIFY_BG)
                .configure_view(strokeOpacity=0)
                .configure_axis(
                    labelColor=SPOTIFY_MUTED,
                    titleColor=SPOTIFY_MUTED,
                    gridColor=SPOTIFY_BORDER,
                    tickColor=SPOTIFY_BORDER,
                    domainColor=SPOTIFY_BORDER,
                )
                .configure_legend(labelColor=SPOTIFY_MUTED, titleColor=SPOTIFY_MUTED)
            )

            st.altair_chart(ch, width="stretch")

    # -----------------------
    # 3) UNIQUE TRACKS: stacked bars
    # -----------------------
    else:
        uniq_all = dnr.groupby(["bucket"])["track_id"].nunique().reset_index(name="uniq_all")
        uniq_new = dnr[dnr["is_new"]].groupby(["bucket"])["track_id"].nunique().reset_index(name="uniq_new")
        agg_u = uniq_all.merge(uniq_new, on="bucket", how="left").fillna({"uniq_new": 0})
        agg_u["uniq_repeat"] = (agg_u["uniq_all"] - agg_u["uniq_new"]).clip(lower=0)

        agg = pd.concat(
            [
                agg_u[["bucket"]].assign(type="New", value=agg_u["uniq_new"]),
                agg_u[["bucket"]].assign(type="Repeat", value=agg_u["uniq_repeat"]),
            ],
            ignore_index=True,
        )

        if agg.empty:
            st.info("Not enough data for the selected range.")
        else:
            agg["bucket_dt"] = pd.to_datetime(agg["bucket"]).dt.normalize()

            color_scale = alt.Scale(domain=["New", "Repeat"], range=[SPOTIFY_GREEN, SPOTIFY_BORDER])
            tooltip_nr = [
                period_tooltip(),
                alt.Tooltip("type:N", title="Type"),
                alt.Tooltip("value:Q", title="Unique tracks", format=",d"),
            ]

            ch = (
                alt.Chart(agg)
                .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
                .encode(
                    x=x_bucket(grain),
                    y=alt.Y("value:Q", title="Unique tracks", stack=True),
                    color=alt.Color("type:N", title=None, scale=color_scale),
                    tooltip=tooltip_nr,
                )
                .properties(height=360, background=SPOTIFY_BG)
                .configure_view(strokeOpacity=0)
                .configure_axis(
                    labelColor=SPOTIFY_MUTED,
                    titleColor=SPOTIFY_MUTED,
                    gridColor=SPOTIFY_BORDER,
                    tickColor=SPOTIFY_BORDER,
                    domainColor=SPOTIFY_BORDER,
                )
                .configure_legend(labelColor=SPOTIFY_MUTED, titleColor=SPOTIFY_MUTED)
            )

            st.altair_chart(ch, width="stretch")