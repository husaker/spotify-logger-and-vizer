from __future__ import annotations

import base64
import json
import re
from datetime import datetime, timedelta, timezone, time
from typing import Any

import altair as alt
import gspread
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from dateutil import parser as dtparser
from zoneinfo import ZoneInfo
import json as _json

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
if "last_sheet_id" not in st.session_state:
    st.session_state["last_sheet_id"] = None

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
    safe = _json.dumps(url)  # proper JS string escaping
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


def encode_oauth_state(*, sheet_id: str, nonce: str) -> str:
    payload = {"sid": sheet_id, "n": nonce}
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def decode_oauth_state(state: str) -> dict[str, str] | None:
    try:
        s = (state or "").strip()
        if not s:
            return None
        pad = "=" * (-len(s) % 4)
        raw = base64.urlsafe_b64decode((s + pad).encode("utf-8"))
        obj = json.loads(raw.decode("utf-8"))
        if not isinstance(obj, dict):
            return None
        sid = str(obj.get("sid") or "").strip()
        nonce = str(obj.get("n") or "").strip()
        if not sid or not nonce:
            return None
        return {"sid": sid, "nonce": nonce}
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
# Registry helpers
# -----------------------------
def get_registry_ws_best_effort(*, sheets: SheetsClient, settings) -> Any | None:
    """
    Returns registry worksheet or None if unavailable.
    """
    try:
        registry_ss = sheets.open_by_key(settings.registry_sheet_id)
        registry_ws = sheets.get_or_create_worksheet(registry_ss, REGISTRY_TAB, rows=2000, cols=20)
        ensure_registry_headers(registry_ws)
        return registry_ws
    except Exception:
        return None


def registry_get_sheet_status(registry_ws, user_sheet_id: str) -> tuple[bool, bool]:
    """
    Returns: (registered, enabled_in_registry)
    """
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
@st.cache_data(ttl=45, show_spinner=False)
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
    rows = cached_ws_values(settings.google_service_account_json, sheet_id, "__cache_tracks", st.session_state["refresh_key"])
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
    rows = cached_ws_values(settings.google_service_account_json, sheet_id, "__cache_artists", st.session_state["refresh_key"])
    df = df_from_ws_rows(rows)
    if df.empty:
        return pd.DataFrame(columns=["artist_id", "artist_name", "artist_cover_url", "genres", "primary_genre", "fetched_at"])
    return df


def load_cache_albums_df(settings, sheet_id: str) -> pd.DataFrame:
    rows = cached_ws_values(settings.google_service_account_json, sheet_id, "__cache_albums", st.session_state["refresh_key"])
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

# Default is collapsed (expanded=False)
with st.expander("How to prepare Google Sheet", expanded=False):
    st.write("1) Create a Google Sheet (or use an existing one).")
    if service_email:
        st.write("2) Click **Share** → add this email as **Editor**:")
        st.code(service_email)
    else:
        st.warning("Could not read client_email from service account JSON. Check GOOGLE_SERVICE_ACCOUNT_JSON.")
    st.write("3) Paste the sheet link/ID below.")
    st.write("4) Connect Spotify (OAuth).")
    st.write("5) Click **Enable background sync** to allow the cron worker to write logs into your sheet.")

st.divider()

# -----------------------------
# Sheet input (supports ?sheet=... auto-fill)
# -----------------------------
sheet_from_qp = get_query_param("sheet")
if sheet_from_qp and "sheet_input" not in st.session_state:
    st.session_state["sheet_input"] = sheet_from_qp

sheet_input = st.text_input(
    "Paste your Google Sheet URL or Sheet ID",
    key="sheet_input",
    placeholder="https://docs.google.com/spreadsheets/d/<ID>/edit ...",
)

sheet_id = extract_sheet_id(sheet_input)

# Reset dashboard render when user switches to another sheet
if sheet_id and st.session_state.get("last_sheet_id") and sheet_id != st.session_state["last_sheet_id"]:
    st.session_state["render_dashboard"] = False
    st.session_state["refresh_key"] += 1
    try:
        st.cache_data.clear()
    except Exception:
        pass

st.session_state["last_sheet_id"] = sheet_id

if not sheet_id:
    st.info("Paste a Google Sheet link/ID to continue.")
    st.stop()

# Open user sheet
try:
    ss = sheets.open_by_key(sheet_id)
except Exception as e:
    st.error("❌ Can't open this Google Sheet with the service account.")
    st.write("Reason:", str(e))
    st.stop()

st.success("Google Sheet is accessible to the service account")

# Ensure structure exists
try:
    ensure_user_sheet_initialized(ss, timezone_name="UTC")
except Exception as e:
    st.error("❌ Failed to initialize the required worksheets (log, etc).")
    st.write("Reason:", str(e))
    st.stop()

st.success("Sheet structure OK (log, caches, app_state)")

# -----------------------------
# OAuth callback handling (state carries sheet_id; auto-sets ?sheet=...)
# -----------------------------
redirect_uri = settings.public_app_url.rstrip("/")

code = get_query_param("code")
state_cb = get_query_param("state")
error_cb = get_query_param("error")

if error_cb:
    st.error(f"Spotify auth error: {error_cb}")
    clear_query_params()
    st.stop()

if code and state_cb:
    decoded = decode_oauth_state(state_cb)
    if not decoded:
        st.error("Invalid OAuth state. Click Connect Spotify again.")
        clear_query_params()
        st.stop()

    sid_from_state = decoded["sid"]

    # Always open the sheet referenced by OAuth state (works even if callback happens in a new tab)
    try:
        ss_cb = sheets.open_by_key(sid_from_state)
        ensure_user_sheet_initialized(ss_cb, timezone_name="UTC")
    except Exception as e:
        st.error("OAuth callback: cannot open/initialize the sheet from state.")
        st.write("Reason:", str(e))
        clear_query_params()
        st.stop()

    sheet_state = read_app_state(ss_cb)
    expected = (sheet_state.get("oauth_state") or "").strip()

    if not expected or expected != state_cb:
        st.error("OAuth state mismatch. Click Connect Spotify again.")
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
                "oauth_state": "",
                "last_error": "",
            },
        )

    # Land back on the main UI with the correct sheet pre-filled
    clear_query_params()
    set_query_params(sheet=sid_from_state)

    st.success("Spotify connected! Returning to your sheet…")
    st.rerun()

# -----------------------------
# Read state + registry
# -----------------------------
state = read_app_state(ss)
enabled_local = (state.get("enabled") or "false").lower() == "true"
timezone_name = (state.get("timezone") or "UTC").strip() or "UTC"
spotify_connected = bool((state.get("refresh_token_enc") or "").strip())
spotify_user_id = (state.get("spotify_user_id") or "").strip()

registry_ws = get_registry_ws_best_effort(sheets=sheets, settings=settings)
registered, enabled_registry = (False, False)
if registry_ws is not None:
    registered, enabled_registry = registry_get_sheet_status(registry_ws, sheet_id)
background_sync_on = registered and enabled_registry

existing_sheet_for_user: str | None = None
if spotify_connected and spotify_user_id and registry_ws is not None:
    try:
        existing_sheet_for_user = find_sheet_by_spotify_user_id(registry_ws, spotify_user_id)
    except Exception:
        existing_sheet_for_user = None

# -----------------------------
# Status
# -----------------------------
st.markdown("## Status")
c1, c2, c3, c4, c5 = st.columns([1.1, 1.1, 1.1, 1.7, 1.5])

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
        st.markdown(f'<span class="badge">Spotify user id</span> <span class="badge">{spotify_user_id}</span>', unsafe_allow_html=True)
with c5:
    st.markdown(
        f'<span class="badge">Background sync</span> <span class="success-pill">ON</span>'
        if background_sync_on
        else f'<span class="badge">Background sync</span> <span class="badge">OFF</span>',
        unsafe_allow_html=True,
    )

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

if not spotify_connected:
    st.warning("Spotify is not connected yet. Connect via OAuth.")

    # If we already generated an auth url on previous run — redirect now
    if st.session_state.get("pending_auth_url"):
        url = st.session_state.pop("pending_auth_url")
        redirect_same_tab(url)

        # Fallback if browser blocks JS navigation
        st.link_button("If you are not redirected, click here", url)
        st.stop()

    if st.button("Connect Spotify"):
        import secrets

        oauth_state = encode_oauth_state(sheet_id=sheet_id, nonce=secrets.token_urlsafe(10))

        # Save oauth_state to __app_state (can hit 429 sometimes)
        try:
            write_app_state_kv(ss, {"oauth_state": oauth_state})
        except gspread.exceptions.APIError as e:
            msg = str(e)
            if "429" in msg or "Quota" in msg or "quota" in msg:
                st.warning(
                    "Google Sheets API rate limit hit while saving oauth_state.\n\n"
                    "Wait ~60 seconds, click **Refresh data**, then try **Connect Spotify** again."
                )
            else:
                st.error("Failed to write oauth_state into __app_state.")
                st.write(msg)
            st.stop()

        scopes = ["user-read-recently-played", "user-read-email", "user-read-private"]
        url = build_auth_url(
            client_id=settings.spotify_client_id,
            redirect_uri=redirect_uri,
            scopes=scopes,
            state=oauth_state,
        )

        # Store url, rerun, then redirect on the next run (more reliable in Streamlit Cloud)
        st.session_state["pending_auth_url"] = url
        st.rerun()

else:
    st.success("Spotify is connected")


    col1, col2, col3, col4 = st.columns([1.4, 1.4, 1.3, 1.9])

    # Enable background sync
    with col1:
        if not background_sync_on:
            if st.button("Enable background sync"):
                if registry_ws is None:
                    st.error("Registry sheet is not accessible to the service account. Cron cannot work.")
                    st.stop()
                if not spotify_user_id:
                    st.error("spotify_user_id is missing in __app_state. Reconnect Spotify.")
                    st.stop()

                existing = find_sheet_by_spotify_user_id(registry_ws, spotify_user_id)
                if existing and existing != sheet_id:
                    st.error(
                        "This Spotify account is already connected to another sheet.\n\n"
                        f"**Active sheet:** `{existing}`\n\n"
                        "To avoid multiple sheets per user, background sync will not be enabled here."
                    )
                    st.stop()

                # Register & enable (support both upsert_registry_user signatures)
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
                st.success("Background sync enabled")
                st.rerun()

    # Disable background sync
    with col2:
        if background_sync_on:
            if st.button("Disable background sync"):
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
                st.info("Background sync disabled")
                st.rerun()

    # Refresh
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
# Dashboard controls (time range picker + render button)
# -----------------------------
st.markdown("## Dashboard")

if not st.session_state.get("render_dashboard"):
    st.info("Pick a date range in the sidebar and click **Render dashboard**.")
else:
    st.success("Dashboard rendered")

# Load minimal log bounds for defaults
try:
    df_log_bounds = load_log_df_cached(settings, sheet_id)
except gspread.exceptions.APIError as e:
    msg = str(e)
    if "Quota exceeded" in msg or "[429]" in msg or "429" in msg:
        st.warning("Google Sheets quota exceeded (429). Wait ~60 seconds and click **Refresh data**.")
        st.stop()
    raise

if df_log_bounds.empty:
    st.info("No rows in log yet. Wait until the worker appends some plays.")
    st.stop()

min_dt = df_log_bounds["played_at_utc"].min().to_pydatetime()
max_dt = df_log_bounds["played_at_utc"].max().to_pydatetime()

with st.sidebar:
    st.markdown("### Time range")
    preset = st.selectbox(
        "Quick range",
        ["Custom", "Last 7 days", "Last 30 days", "Last 90 days", "All time"],
        index=1,
    )

    today_utc = datetime.now(timezone.utc).date()
    if preset == "Last 7 days":
        default_from = (datetime.now(timezone.utc) - timedelta(days=7)).date()
        default_to = today_utc
    elif preset == "Last 30 days":
        default_from = (datetime.now(timezone.utc) - timedelta(days=30)).date()
        default_to = today_utc
    elif preset == "Last 90 days":
        default_from = (datetime.now(timezone.utc) - timedelta(days=90)).date()
        default_to = today_utc
    elif preset == "All time":
        default_from = min_dt.date()
        default_to = max_dt.date()
    else:
        default_from = (datetime.now(timezone.utc) - timedelta(days=30)).date()
        default_to = today_utc

    picked = st.date_input("From / To", value=(default_from, default_to))
    if isinstance(picked, tuple) and len(picked) == 2:
        date_from, date_to = picked
    else:
        date_from = picked
        date_to = picked

    st.markdown('<div class="small-muted">Rendering happens only on button click.</div>', unsafe_allow_html=True)
    if st.button("▶ Render dashboard", use_container_width=True):
        st.session_state["render_dashboard"] = True
        st.session_state["refresh_key"] += 1
        st.rerun()

# Stop early unless user pressed render
if not st.session_state.get("render_dashboard"):
    st.stop()

# -----------------------------
# Load data (log + caches)
# -----------------------------
try:
    df_log = load_log_df_cached(settings, sheet_id)
    df_ct = load_cache_tracks_df(settings, sheet_id)
    df_ca = load_cache_artists_df(settings, sheet_id)
    df_calb = load_cache_albums_df(settings, sheet_id)
except gspread.exceptions.APIError as e:
    msg = str(e)
    if "Quota exceeded" in msg or "[429]" in msg or "429" in msg:
        st.warning("Google Sheets quota exceeded (429). Wait ~60 seconds and click **Refresh data**.")
        st.stop()
    raise

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

st.divider()

# -----------------------------
# Tabs
# -----------------------------
tab_artists, tab_tracks, tab_albums, tab_monthly, tab_genres, tab_stats, tab_recent = st.tabs(
    ["Top 5 Artists", "Top 5 Tracks", "Top 5 Albums", "Monthly avg", "Top 5 Genres", "Statistics", "Recent plays"]
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
    st.markdown("### Average per active day by month")

    dfm = df.copy()
    dfm["month"] = dfm["played_at_utc"].dt.to_period("M").dt.to_timestamp()

    # Active days per month (days with >=1 play)
    active_days = (
        dfm.groupby("month")["played_at_utc"]
        .apply(lambda s: s.dt.date.nunique())
        .reset_index(name="active_days")
    )

    # Month-level totals
    month_agg = (
        dfm.groupby("month")
        .agg(plays=("track_id", "count"), minutes=("minutes", "sum"))
        .reset_index()
        .sort_values("month")
    )

    month_agg = month_agg.merge(active_days, on="month", how="left")
    month_agg["active_days"] = month_agg["active_days"].fillna(0).astype(int)

    # Avoid division by zero
    month_agg["avg_tracks_per_active_day"] = month_agg.apply(
        lambda r: (r["plays"] / r["active_days"]) if r["active_days"] > 0 else 0.0,
        axis=1,
    )
    month_agg["avg_minutes_per_active_day"] = month_agg.apply(
        lambda r: (r["minutes"] / r["active_days"]) if r["active_days"] > 0 else 0.0,
        axis=1,
    )

    # Top album per month (for cover markers)
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

    base = alt.Chart(plays_m).encode(
        x=alt.X("month_str:N", title=None),
    )

    line = base.mark_line(color=SPOTIFY_GREEN).encode(
        y=alt.Y("avg_tracks_per_active_day:Q", title="Avg tracks / active day"),
        tooltip=["month_str", "avg_tracks_per_active_day", "avg_minutes_per_active_day", "plays", "active_days"],
    )

    points = base.mark_point(color=SPOTIFY_GREEN, size=70).encode(
        y="avg_tracks_per_active_day:Q",
    )

    img_df = plays_m[plays_m["album_cover_url"].fillna("").astype(str).str.len() > 0].copy()

    covers = alt.Chart(img_df).mark_image(width=22, height=22, dy=-16).encode(
        x="month_str:N",
        y="avg_tracks_per_active_day:Q",
        url="album_cover_url:N",
        tooltip=["month_str", "album_name", "avg_tracks_per_active_day", "plays", "active_days"],
    )

    st.altair_chart((line + points + covers).properties(height=280), use_container_width=True)

    st.markdown("### Table")
    show_tbl = plays_m[
        [
            "month_str",
            "plays",
            "minutes",
            "active_days",
            "avg_tracks_per_active_day",
            "avg_minutes_per_active_day",
            "album_name",
        ]
    ].copy()
    show_tbl["minutes"] = show_tbl["minutes"].round(0).astype(int)
    show_tbl["avg_tracks_per_active_day"] = show_tbl["avg_tracks_per_active_day"].round(2)
    show_tbl["avg_minutes_per_active_day"] = show_tbl["avg_minutes_per_active_day"].round(1)
    st.dataframe(show_tbl, use_container_width=True, hide_index=True)

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
            tooltip=["primary_genre", "plays"],
        )
        .properties(height=300)
    )
    st.altair_chart(ch, use_container_width=True)

# ===== Statistics =====
with tab_stats:
    unique_artists = df["Artist"].nunique()
    unique_tracks = df["track_id"].nunique()
    total_tracks = len(df)
    total_minutes = int(round(df["minutes"].sum(), 0))
    active_days_sel = df["played_at_utc"].dt.date.nunique()

    fav_genre = ""
    if "primary_genre" in df.columns:
        gg = df["primary_genre"].fillna("").astype(str).str.strip()
        gg = gg[gg.str.len() > 0]
        if len(gg) > 0:
            fav_genre = gg.value_counts().index[0]

    st.markdown("### Statistics")
    s1, s2, s3, s4, s5 = st.columns([1, 1, 1, 1, 1])
    with s1:
        kpi_card("Unique artists", str(unique_artists))
    with s2:
        kpi_card("Unique tracks", str(unique_tracks))
    with s3:
        kpi_card("Total tracks played", str(total_tracks))
    with s4:
        kpi_card("Total minutes listened", str(total_minutes))
    with s5:
        kpi_card("Active days", str(active_days_sel))

    if fav_genre:
        st.markdown(f"**Favorite genre:** `{fav_genre}`")
    else:
        st.markdown("**Favorite genre:** _(not enough genre data yet)_")

# ===== Recent plays =====
with tab_recent:
    st.markdown("### Recent plays")
    show_n = st.slider("Rows", min_value=20, max_value=500, value=100, step=20)

    recent = df.sort_values("played_at_utc", ascending=False).head(show_n).copy()
    recent["Played (UTC)"] = recent["played_at_utc"].dt.strftime("%Y-%m-%d %H:%M")
    recent = recent[["Played (UTC)", "Track", "Artist", "track_id", "URL", "album_name", "primary_genre"]].rename(
        columns={"track_id": "Spotify ID"}
    )

    st.dataframe(recent, use_container_width=True, hide_index=True)