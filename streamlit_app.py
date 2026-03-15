from __future__ import annotations

import base64
import hashlib
import html
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
import plotly.graph_objects as go

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
    load_registry_snapshot,
    registry_status_from_snapshot,
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
DISCOVERY_ACCENT = "#95E5A1"

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
.genre-card {{
  position: relative;
  min-height: 188px;
  background:
    radial-gradient(220px 140px at 100% 0%, rgba(29,185,84,0.20) 0%, rgba(29,185,84,0.00) 70%),
    linear-gradient(180deg, rgba(255,255,255,0.035) 0%, rgba(255,255,255,0.02) 100%);
  border: 1px solid {SPOTIFY_BORDER};
  border-radius: 18px;
  padding: 14px;
  overflow: hidden;
}}
.genre-chip {{
  display: inline-block;
  padding: 3px 8px;
  border-radius: 999px;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.04em;
  color: {SPOTIFY_GREEN};
  background: rgba(29,185,84,0.12);
  border: 1px solid rgba(29,185,84,0.28);
}}
.genre-title {{
  margin-top: 10px;
  font-size: 21px;
  font-weight: 800;
  line-height: 1.1;
}}
.genre-stats {{
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 10px;
  margin-top: 12px;
}}
.genre-stat-label {{
  color: {SPOTIFY_MUTED};
  font-size: 11px;
}}
.genre-stat-value {{
  margin-top: 2px;
  font-size: 18px;
  font-weight: 800;
}}
.genre-artists {{
  margin-top: 12px;
}}
.genre-artist-list {{
  margin-top: 8px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}}
.genre-artist {{
  display: flex;
  align-items: center;
  gap: 10px;
}}
.genre-artist-cover {{
  flex: 0 0 auto;
  border-radius: 999px;
  object-fit: cover;
  border: 1px solid rgba(255,255,255,0.12);
  background: linear-gradient(135deg, rgba(29,185,84,0.28) 0%, rgba(255,255,255,0.06) 100%);
}}
.genre-artist-fallback {{
  display: flex;
  align-items: center;
  justify-content: center;
  color: {SPOTIFY_TEXT};
  font-weight: 800;
}}
.genre-artist-cover--lg {{
  width: 46px;
  height: 46px;
}}
.genre-artist-cover--sm {{
  width: 30px;
  height: 30px;
}}
.genre-artist-name {{
  line-height: 1.15;
  font-weight: 700;
}}
.genre-artist-name--lg {{
  font-size: 14px;
}}
.genre-artist-name--sm {{
  font-size: 12px;
}}
.genre-artist-meta {{
  margin-top: 3px;
  color: {SPOTIFY_MUTED};
}}
.genre-artist-meta--lg {{
  font-size: 11px;
}}
.genre-artist-meta--sm {{
  font-size: 10px;
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
if "dashboard_ws_backup" not in st.session_state:
    st.session_state["dashboard_ws_backup"] = {}
if "dashboard_fallback" not in st.session_state:
    st.session_state["dashboard_fallback"] = {"log": False, "meta": False}

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


# -----------------------------
# Cached reads (reduce 429 on reruns)
# -----------------------------
def is_google_quota_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "429" in msg or "quota exceeded" in msg or "rate limit" in msg or "user-rate limit" in msg


def _dashboard_backup_key(sheet_id: str, scope: str) -> str:
    return f"{sheet_id}:{scope}"


def remember_dashboard_backup(sheet_id: str, scope: str, payload: Any) -> None:
    st.session_state["dashboard_ws_backup"][_dashboard_backup_key(sheet_id, scope)] = payload


def get_dashboard_backup(sheet_id: str, scope: str) -> Any | None:
    return (st.session_state.get("dashboard_ws_backup") or {}).get(_dashboard_backup_key(sheet_id, scope))


def _sheet_title_from_a1_range(a1_range: str) -> str:
    title = str(a1_range or "").split("!", 1)[0]
    if title.startswith("'") and title.endswith("'"):
        title = title[1:-1].replace("''", "'")
    return title


@st.cache_data(ttl=90, show_spinner=False)
def cached_log_ws_values(service_json: str, sheet_id: str, refresh_key: int) -> list[list[str]]:
    sheets_local = SheetsClient.from_service_account_json(service_json)
    ss_local = sheets_local.open_by_key(sheet_id)
    ws = ss_local.worksheet("log")
    return gcall(lambda: ws.get_all_values())


def df_from_ws_rows(rows: list[list[str]]) -> pd.DataFrame:
    if not rows or len(rows) < 2:
        return pd.DataFrame()
    header = rows[0]
    data = rows[1:]
    return pd.DataFrame(data, columns=header[: len(header)])


@st.cache_data(ttl=900, show_spinner=False)
def cached_metadata_ws_values(service_json: str, sheet_id: str, refresh_key: int) -> dict[str, list[list[str]]]:
    sheets_local = SheetsClient.from_service_account_json(service_json)
    ss_local = sheets_local.open_by_key(sheet_id)
    ws_titles = ["__cache_tracks", "__cache_artists", "__cache_albums"]
    ranges = [f"'{title}'!A:Z" for title in ws_titles]
    resp = gcall(lambda: ss_local.values_batch_get(ranges))

    rows_by_title: dict[str, list[list[str]]] = {title: [] for title in ws_titles}
    for value_range in resp.get("valueRanges", []):
        title = _sheet_title_from_a1_range(value_range.get("range", ""))
        if title in rows_by_title:
            rows_by_title[title] = value_range.get("values", []) or []
    return rows_by_title


def load_log_rows_resilient(settings, sheet_id: str) -> list[list[str]]:
    try:
        rows = cached_log_ws_values(
            settings.google_service_account_json,
            sheet_id,
            st.session_state["refresh_key"],
        )
        remember_dashboard_backup(sheet_id, "log", rows)
        st.session_state["dashboard_fallback"]["log"] = False
        return rows
    except gspread.exceptions.APIError as e:
        if is_google_quota_error(e):
            backup = get_dashboard_backup(sheet_id, "log")
            if backup is not None:
                st.session_state["dashboard_fallback"]["log"] = True
                return backup
        raise


def load_metadata_rows_resilient(settings, sheet_id: str) -> dict[str, list[list[str]]]:
    try:
        rows_by_title = cached_metadata_ws_values(
            settings.google_service_account_json,
            sheet_id,
            st.session_state["refresh_key"],
        )
        remember_dashboard_backup(sheet_id, "meta", rows_by_title)
        st.session_state["dashboard_fallback"]["meta"] = False
        return rows_by_title
    except gspread.exceptions.APIError as e:
        if is_google_quota_error(e):
            backup = get_dashboard_backup(sheet_id, "meta")
            if backup is not None:
                st.session_state["dashboard_fallback"]["meta"] = True
                return backup
        raise


def load_log_df_cached(settings, sheet_id: str) -> pd.DataFrame:
    rows = load_log_rows_resilient(settings, sheet_id)
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
    rows_by_title = load_metadata_rows_resilient(settings, sheet_id)
    rows = rows_by_title.get("__cache_tracks", [])
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
    rows_by_title = load_metadata_rows_resilient(settings, sheet_id)
    rows = rows_by_title.get("__cache_artists", [])
    df = df_from_ws_rows(rows)
    if df.empty:
        return pd.DataFrame(
            columns=["artist_id", "artist_name", "artist_cover_url", "genres", "primary_genre", "fetched_at"]
        )
    return df


def load_cache_albums_df(settings, sheet_id: str) -> pd.DataFrame:
    rows_by_title = load_metadata_rows_resilient(settings, sheet_id)
    rows = rows_by_title.get("__cache_albums", [])
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


def render_genre_cards(items: list[dict[str, Any]], *, cols: int = 5) -> None:
    if not items:
        st.info("No genre data for the selected range.")
        return

    grid = st.columns(max(1, min(cols, len(items))))
    for i, it in enumerate(items):
        with grid[i % len(grid)]:
            rank = html.escape(str(it.get("rank") or ""))
            title = html.escape(str(it.get("title") or ""))
            plays = html.escape(str(it.get("plays") or "0"))
            share = html.escape(str(it.get("share") or "0%"))
            minutes = html.escape(str(it.get("minutes") or "0"))
            artists = it.get("artists") or []

            artist_rows_html: list[str] = []
            for idx, artist in enumerate(artists[:2]):
                artist_name_raw = str(artist.get("name") or "(Unknown artist)")
                artist_name = html.escape(artist_name_raw)
                artist_plays = html.escape(str(artist.get("plays") or "0"))
                artist_cover = str(artist.get("cover") or "").strip()
                size_cls = "genre-artist-cover--lg" if idx == 0 else "genre-artist-cover--sm"
                name_cls = "genre-artist-name--lg" if idx == 0 else "genre-artist-name--sm"
                meta_cls = "genre-artist-meta--lg" if idx == 0 else "genre-artist-meta--sm"
                initial = html.escape((artist_name_raw.strip()[:1] or "?").upper())

                if artist_cover:
                    cover_html = (
                        f'<img class="genre-artist-cover {size_cls}" '
                        f'src="{html.escape(artist_cover)}" alt="{artist_name}" />'
                    )
                else:
                    cover_html = (
                        f'<div class="genre-artist-cover genre-artist-fallback {size_cls}">{initial}</div>'
                    )

                artist_rows_html.append(
                    f"""
<div class="genre-artist">
  {cover_html}
  <div>
    <div class="genre-artist-name {name_cls}">{artist_name}</div>
    <div class="genre-artist-meta {meta_cls}">{artist_plays} plays</div>
  </div>
</div>
"""
                )

            artists_html = "".join(artist_rows_html)

            st.markdown(
                f"""
<div class="genre-card">
  <div class="genre-chip">{rank}</div>
  <div class="genre-title">{title}</div>
  <div class="small-muted" style="margin-top:8px;">{share} of plays</div>
  <div class="genre-stats">
    <div>
      <div class="genre-stat-label">Plays</div>
      <div class="genre-stat-value">{plays}</div>
    </div>
    <div>
      <div class="genre-stat-label">Minutes</div>
      <div class="genre-stat-value">{minutes}</div>
    </div>
  </div>
  <div class="genre-artists">
    <div class="small-muted">Top artists</div>
    <div class="genre-artist-list">{artists_html}</div>
  </div>
</div>
""",
                unsafe_allow_html=True,
            )

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
    tz,
    days: int = 365,
    cell_px: int = 18,
    gap_px: int = 3,
) -> None:
    """
    Activity grid (last N days).
    Fast version: single Scattergl trace (no layout.shapes).
    Fixes uneven spacing by locking x/y scale (scaleanchor),
    while keeping layout tight (labels in paper coords).
    """
    import numpy as np
    import pandas as pd
    import plotly.graph_objects as go
    from datetime import timedelta

    st.markdown(f"### Activity (last {days} days)")

    if df_log is None or df_log.empty or "played_at_utc" not in df_log.columns:
        st.info("No activity data yet.")
        return

    # --- base
    base = df_log[["played_at_utc", "Spotify ID"]].copy()
    base = base[base["played_at_utc"].notna()].copy()
    base["track_id"] = base["Spotify ID"].astype(str).fillna("")
    base = base[base["track_id"].str.len() > 0].copy()

    try:
        base["played_local"] = base["played_at_utc"].dt.tz_convert(tz)
    except Exception:
        base["played_at_utc"] = pd.to_datetime(base["played_at_utc"], utc=True, errors="coerce")
        base = base[base["played_at_utc"].notna()].copy()
        base["played_local"] = base["played_at_utc"].dt.tz_convert(tz)

    base["day"] = base["played_local"].dt.date

    today_local = pd.Timestamp.now(tz).date()
    start = (pd.Timestamp(today_local) - pd.Timedelta(days=days - 1)).date()
    end = today_local

    base = base[(base["day"] >= start) & (base["day"] <= end)].copy()
    if base.empty:
        st.info(f"No activity data in the last {days} days.")
        return

    daily = (
        base.groupby("day", dropna=False)
        .agg(plays=("track_id", "size"))
        .reset_index()
    )

    grid = pd.DataFrame({"day": pd.date_range(start, end, freq="D").date})
    grid = grid.merge(daily, on="day", how="left").fillna({"plays": 0})
    grid["plays"] = grid["plays"].astype(int)

    # --- coords
    first_monday = start - timedelta(days=start.weekday())
    grid["day_ts"] = pd.to_datetime(grid["day"])  # naive timestamp
    grid["day_name"] = grid["day_ts"].dt.day_name()
    grid["dow"] = grid["day_ts"].dt.weekday.astype(int)  # Mon=0..Sun=6

    first_monday_ts = pd.Timestamp(first_monday)
    grid["week"] = ((grid["day_ts"] - first_monday_ts).dt.days // 7).astype(int)
    n_weeks = int(grid["week"].max()) + 1

    # --- levels (0..4)
    grid["level"] = 0
    pos = grid["plays"] > 0
    if pos.any():
        pos_vals = grid.loc[pos, "plays"]
        try:
            qlvl = pd.qcut(pos_vals, q=4, labels=[1, 2, 3, 4], duplicates="drop").astype(int)
            grid.loc[pos, "level"] = qlvl.values
        except Exception:
            pct = pos_vals.rank(pct=True, method="average")
            grid.loc[pos, "level"] = (pct * 4.0).apply(
                lambda x: max(1, min(4, int(x) if float(x).is_integer() else int(x) + 1))
            ).astype(int)

    def rgba(hex_color: str, a: float) -> str:
        h = hex_color.lstrip("#")
        r = int(h[0:2], 16)
        g = int(h[2:4], 16)
        b = int(h[4:6], 16)
        return f"rgba({r},{g},{b},{a})"

    palette = {
        0: rgba(SPOTIFY_BORDER, 1.0),
        1: rgba(SPOTIFY_GREEN, 0.25),
        2: rgba(SPOTIFY_GREEN, 0.45),
        3: rgba(SPOTIFY_GREEN, 0.65),
        4: rgba(SPOTIFY_GREEN, 1.00),
    }

    step = cell_px + gap_px
    grid["x"] = grid["week"] * step
    grid["y"] = (6 - grid["dow"]) * step

    colors = grid["level"].map(lambda v: palette[int(v)]).tolist()

    customdata = np.stack(
        [
            grid["day_ts"].dt.strftime("%Y-%m-%d").to_numpy(),
            grid["day_name"].to_numpy(),
            grid["plays"].to_numpy(),
        ],
        axis=1,
    )

    fig = go.Figure(
        go.Scattergl(
            x=grid["x"],
            y=grid["y"],
            mode="markers",
            marker=dict(
                symbol="square",
                size=cell_px,
                color=colors,
                line=dict(width=1, color=SPOTIFY_BG),
            ),
            customdata=customdata,
            hovertemplate="<b>%{customdata[0]}</b> (%{customdata[1]})<br>"
                          "Plays: <b>%{customdata[2]}</b><extra></extra>",
        )
    )

    # --- RANGES: only the grid itself (so no vertical "mystery whitespace")
    x_min = -0.5 * step
    x_max = (n_weeks - 1) * step + 0.5 * step
    y_min = -0.5 * step
    y_max = 6 * step + 0.5 * step

    # --- Month labels (top) in PAPER coords (do not affect y-range)
    month_starts = pd.date_range(start, end, freq="MS").date
    used = set()
    for m in month_starts:
        w = (m - first_monday).days // 7
        if w in used:
            continue
        used.add(w)
        fig.add_annotation(
            x=w * step - (cell_px / 2.0),
            xref="x",
            y=1.06,              # slightly above plot area
            yref="paper",
            text=pd.Timestamp(m).strftime("%b"),
            showarrow=False,
            font=dict(color=SPOTIFY_MUTED, size=16),
            xanchor="left",
            yanchor="middle",
        )

    # --- Left labels in PAPER X (do not affect x-range)
    for dow, txt in {0: "Mon", 2: "Wed", 4: "Fri"}.items():
        fig.add_annotation(
            x=-0.02,            # a bit left of plot area
            xref="paper",
            y=(6 - dow) * step,
            yref="y",
            text=txt,
            showarrow=False,
            font=dict(color=SPOTIFY_MUTED, size=16),
            xanchor="right",
            yanchor="middle",
        )

    # --- Height tight to grid (anchor will keep x/y scale equal => gaps одинаковые)
    # 7 rows * step + a bit for month labels area
    height = int(7 * step + 36)

    fig.update_layout(
        paper_bgcolor=SPOTIFY_BG,
        plot_bgcolor=SPOTIFY_BG,
        margin=dict(l=58, r=16, t=26, b=8),
        height=height,
        xaxis=dict(
            visible=False,
            range=[x_min, x_max],
            fixedrange=True,
            constrain="domain",
        ),
        yaxis=dict(
            visible=False,
            range=[y_min, y_max],
            fixedrange=True,
            scaleanchor="x",
            scaleratio=1,
            constrain="domain",
        ),
    )

    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

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
        try:
            registry_snapshot = load_registry_snapshot(registry_ws)
            registered, enabled_registry = registry_status_from_snapshot(registry_snapshot, sheet_id)
            if spotify_connected and spotify_user_id:
                existing_sheet_for_user = find_sheet_by_spotify_user_id(
                    registry_ws,
                    spotify_user_id,
                    snapshot=registry_snapshot,
                )
        except Exception:
            registered, enabled_registry = (False, False)
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
                registry_snapshot = None
                try:
                    registry_snapshot = load_registry_snapshot(registry_ws)
                    existing = find_sheet_by_spotify_user_id(
                        registry_ws,
                        spotify_user_id,
                        snapshot=registry_snapshot,
                    )
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
                        snapshot=registry_snapshot,
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

                registry_snapshot = None
                try:
                    registry_snapshot = load_registry_snapshot(registry_ws)
                except Exception:
                    registry_snapshot = None

                try:
                    upsert_registry_user(
                        registry_ws,
                        user_sheet_id=sheet_id,
                        enabled=False,
                        spotify_user_id=spotify_user_id or None,
                        snapshot=registry_snapshot,
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

    presets = ["All time", "This year", "Last 7 days", "Last 30 days", "Last 90 days", "Custom"]
    preset = st.selectbox("Quick range", presets, index=0)

    today_utc = datetime.now(timezone.utc).date()
    current_year = today_utc.year

    min_data_date = st.session_state.get("min_data_date")  # may be None until we load df_log once

    if preset == "All time":
        default_from = min_data_date or datetime(current_year - 1, 1, 1, tzinfo=timezone.utc).date()
        default_to = today_utc

    elif preset == "This year":
        default_from = datetime(current_year, 1, 1, tzinfo=timezone.utc).date()
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
st.session_state["dashboard_fallback"] = {"log": False, "meta": False}

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

    # Prepare full-history mapping for Discovery vs Replay (do NOT depend on selected range)
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
    if is_google_quota_error(e) or "Quota exceeded" in msg or "[429]" in msg or "429" in msg:
        st.warning("Google Sheets quota exceeded (429). Wait ~60 seconds and click **Refresh data**.")
        st.stop()
    raise

fallback_state = st.session_state.get("dashboard_fallback") or {}
if fallback_state.get("log") or fallback_state.get("meta"):
    st.warning("Google Sheets quota was exceeded, so the dashboard is showing the last cached snapshot for this sheet.")

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
tab_artists, tab_tracks, tab_albums, tab_genres, tab_plays, tab_discovery_replay, tab_fingerprint = st.tabs(
    ["Top 5 Artists", "Top 5 Tracks", "Top 5 Albums", "Top 5 Genres", "Plays", "Discovery vs Replay", "Listening fingerprint"]
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

# ===== Plays =====
with tab_plays:
    st.markdown("### Plays")

    plays_grain = st.selectbox("Granularity", ["Week", "Month"], index=0, key="plays_grain")

    dfp = df.copy()
    played_local_naive = dfp["played_at_utc"].dt.tz_convert(tz).dt.tz_localize(None)

    if plays_grain == "Month":
        dfp["bucket_dt"] = played_local_naive.dt.to_period("M").dt.to_timestamp()
    else:
        played_day = played_local_naive.dt.normalize()
        dfp["bucket_dt"] = (played_day - pd.to_timedelta(played_day.dt.weekday, unit="D")).dt.normalize()

    plays_agg = (
        dfp.groupby("bucket_dt", dropna=False)
           .agg(plays=("track_id", "count"), minutes=("minutes", "sum"))
           .reset_index()
           .sort_values("bucket_dt")
    )

    top_track = (
        dfp.groupby(["bucket_dt", "track_id", "Track", "Artist"], dropna=False)
           .size()
           .reset_index(name="plays_track")
           .sort_values(["bucket_dt", "plays_track", "Track", "Artist"], ascending=[True, False, True, True])
    )
    top_track = top_track.groupby("bucket_dt").head(1)

    cover_by_track = (
        dfp.groupby("track_id")["track_cover_url"]
           .agg(lambda s: next((x for x in s if isinstance(x, str) and x.strip()), ""))
           .to_dict()
    )
    top_track["track_cover_url"] = top_track["track_id"].map(cover_by_track)

    plays_view = plays_agg.merge(
        top_track[["bucket_dt", "Track", "Artist", "plays_track", "track_cover_url"]],
        on="bucket_dt",
        how="left",
    )

    tooltip_main = [
        period_tooltip(),
        alt.Tooltip("plays:Q", title="Plays", format=",d"),
        alt.Tooltip("minutes:Q", title="Minutes", format=".1f"),
        alt.Tooltip("Track:N", title="Top track"),
        alt.Tooltip("Artist:N", title="Artist"),
        alt.Tooltip("plays_track:Q", title="Top track plays", format=",d"),
    ]

    line = (
        alt.Chart(plays_view)
        .mark_line(color=SPOTIFY_GREEN, strokeWidth=3)
        .encode(
            x=x_bucket(plays_grain),
            y=alt.Y("plays:Q", title="Plays"),
        )
    )

    points = (
        alt.Chart(plays_view)
        .mark_circle(size=90, color=SPOTIFY_GREEN, stroke=SPOTIFY_BG, strokeWidth=2)
        .encode(
            x=x_bucket(plays_grain),
            y=alt.Y("plays:Q", title="Plays"),
            tooltip=tooltip_main,
        )
    )

    img_df = plays_view[plays_view["track_cover_url"].fillna("").astype(str).str.len() > 0].copy()

    tooltip_cover = [
        period_tooltip(),
        alt.Tooltip("Track:N", title="Top track"),
        alt.Tooltip("Artist:N", title="Artist"),
        alt.Tooltip("plays_track:Q", title="Top track plays", format=",d"),
        alt.Tooltip("plays:Q", title="Plays", format=",d"),
    ]

    covers = (
        alt.Chart(img_df)
        .mark_image(width=42, height=42, dy=-30)
        .encode(
            x=x_bucket(plays_grain),
            y="plays:Q",
            url="track_cover_url:N",
            tooltip=tooltip_cover,
        )
    )

    plays_chart = (
        alt.layer(line, points, covers)
        .properties(height=500, background=SPOTIFY_BG)
        .configure_view(strokeOpacity=0)
        .configure_axis(
            labelColor=SPOTIFY_MUTED,
            titleColor=SPOTIFY_MUTED,
            grid=False,
            tickColor=SPOTIFY_BORDER,
            domainColor=SPOTIFY_BORDER,
        )
    )

    st.altair_chart(plays_chart, width="stretch")


# ===== Top 5 Genres =====
with tab_genres:
    gen = df.copy()
    gen["primary_genre"] = gen.get("primary_genre", "").fillna("").astype(str).str.strip()
    gen = gen[gen["primary_genre"].str.len() > 0].copy()

    if gen.empty:
        st.info("Genres are empty (artist cache may not be filled yet). Run cache enrichment/backfill.")
    else:
        total_plays = len(gen)
        g = (
            gen.groupby("primary_genre", dropna=False)
            .agg(
                plays=("track_id", "count"),
                minutes=("minutes", "sum"),
            )
            .reset_index()
            .sort_values(["plays", "minutes", "primary_genre"], ascending=[False, False, True])
            .head(5)
        )

        items: list[dict[str, Any]] = []
        for rank, (_, row) in enumerate(g.iterrows(), start=1):
            genre_name = str(row["primary_genre"])
            plays = int(row["plays"])
            minutes = int(round(float(row["minutes"]), 0))
            share_pct = (plays / total_plays * 100.0) if total_plays > 0 else 0.0

            genre_slice = gen[gen["primary_genre"] == genre_name].copy()
            top_artists = (
                genre_slice.groupby("Artist", dropna=False)
                .agg(
                    plays=("track_id", "count"),
                    minutes=("minutes", "sum"),
                    cover=("artist_cover_best", lambda s: next((x for x in s if isinstance(x, str) and x.strip()), "")),
                )
                .reset_index()
                .sort_values(["plays", "minutes", "Artist"], ascending=[False, False, True])
                .head(2)
            )

            artist_items = [
                {
                    "name": str(a["Artist"]).strip() or "(Unknown artist)",
                    "plays": f"{int(a['plays']):,}",
                    "cover": str(a["cover"] or "").strip(),
                }
                for _, a in top_artists.iterrows()
            ]

            items.append(
                {
                    "rank": rank,
                    "title": genre_name,
                    "plays": f"{plays:,}",
                    "share": f"{share_pct:.1f}%",
                    "minutes": f"{minutes:,}",
                    "artists": artist_items,
                }
            )

        render_genre_cards(items, cols=5)

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

# ===== Discovery vs Replay =====
with tab_discovery_replay:
    st.markdown("### Discovery vs Replay")
    st.markdown(
        '<div class="small-muted">New = first time a track appears in your whole log. Repeat = all other plays. Exploration score is shown above each bar as the share of new unique tracks.</div>',
        unsafe_allow_html=True,
    )

    grain = st.selectbox("Granularity", ["Week", "Month"], index=0, key="discovery_replay_grain")

    df_dr = df[["track_id", "played_at_utc"]].copy()
    df_dr = df_dr.merge(first_play, on="track_id", how="left")

    # Bucket in LOCAL time (naive for to_period)
    played_local_naive = df_dr["played_at_utc"].dt.tz_convert(tz).dt.tz_localize(None)
    first_local_naive = df_dr["first_play_utc"].dt.tz_convert(tz).dt.tz_localize(None)

    if grain == "Month":
        df_dr["bucket"] = played_local_naive.dt.to_period("M").dt.to_timestamp()
        df_dr["first_bucket"] = first_local_naive.dt.to_period("M").dt.to_timestamp()
    else:
        # week starting Monday
        played_day = played_local_naive.dt.normalize()
        first_day = first_local_naive.dt.normalize()
        df_dr["bucket"] = (played_day - pd.to_timedelta(played_day.dt.weekday, unit="D")).dt.normalize()
        df_dr["first_bucket"] = (first_day - pd.to_timedelta(first_day.dt.weekday, unit="D")).dt.normalize()

    df_dr["is_new"] = df_dr["bucket"] == df_dr["first_bucket"]

    uniq_all = df_dr.groupby(["bucket"])["track_id"].nunique().reset_index(name="uniq_all")
    uniq_new = df_dr[df_dr["is_new"]].groupby(["bucket"])["track_id"].nunique().reset_index(name="uniq_new")
    agg_u = uniq_all.merge(uniq_new, on="bucket", how="left").fillna({"uniq_new": 0})
    agg_u["uniq_repeat"] = (agg_u["uniq_all"] - agg_u["uniq_new"]).clip(lower=0)

    if agg_u.empty:
        st.info("Not enough data for the selected range.")
    else:
        agg_u["bucket_dt"] = pd.to_datetime(agg_u["bucket"]).dt.normalize()
        agg_u["exploration_score"] = agg_u.apply(
            lambda r: (r["uniq_new"] / r["uniq_all"]) if r["uniq_all"] > 0 else 0.0,
            axis=1,
        )
        agg_u["exploration_score_label"] = agg_u["exploration_score"].map(lambda x: f"{x * 100:.0f}%")

        bars_df = pd.concat(
            [
                agg_u[["bucket", "bucket_dt", "uniq_all", "exploration_score"]].assign(
                    type="New",
                    value=agg_u["uniq_new"],
                ),
                agg_u[["bucket", "bucket_dt", "uniq_all", "exploration_score"]].assign(
                    type="Repeat",
                    value=agg_u["uniq_repeat"],
                ),
            ],
            ignore_index=True,
        )

        color_scale = alt.Scale(domain=["New", "Repeat"], range=[SPOTIFY_GREEN, SPOTIFY_BORDER])
        tooltip_bars = [
            period_tooltip(),
            alt.Tooltip("type:N", title="Type"),
            alt.Tooltip("value:Q", title="Unique tracks", format=",d"),
            alt.Tooltip("uniq_all:Q", title="Total unique tracks", format=",d"),
            alt.Tooltip("exploration_score:Q", title="Exploration score", format=".1%"),
        ]
        tooltip_line = [
            period_tooltip(),
            alt.Tooltip("exploration_score:Q", title="Exploration score", format=".1%"),
            alt.Tooltip("uniq_new:Q", title="New unique tracks", format=",d"),
            alt.Tooltip("uniq_repeat:Q", title="Repeat unique tracks", format=",d"),
            alt.Tooltip("uniq_all:Q", title="Total unique tracks", format=",d"),
        ]

        bars = (
            alt.Chart(bars_df)
            .mark_bar(cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
            .encode(
                x=x_bucket(grain),
                y=alt.Y("value:Q", title="Unique tracks", stack=True),
                color=alt.Color(
                    "type:N",
                    title=None,
                    scale=color_scale,
                    legend=alt.Legend(
                        orient="top-right",
                        direction="vertical",
                        fillColor=SPOTIFY_CARD,
                        strokeColor=SPOTIFY_BORDER,
                        padding=8,
                        cornerRadius=10,
                        labelColor=SPOTIFY_TEXT,
                        symbolType="square",
                        symbolSize=90,
                    ),
                ),
                tooltip=tooltip_bars,
            )
        )

        score_labels = (
            alt.Chart(agg_u)
            .mark_text(color=DISCOVERY_ACCENT, fontSize=12, fontWeight=700, dy=-10)
            .encode(
                x=x_bucket(grain),
                y=alt.Y("uniq_all:Q"),
                text="exploration_score_label:N",
                tooltip=tooltip_line,
            )
        )

        ch = (
            alt.layer(bars, score_labels)
            .properties(height=520, background=SPOTIFY_BG)
            .configure_view(strokeOpacity=0)
            .configure_axis(
                labelColor=SPOTIFY_MUTED,
                titleColor=SPOTIFY_MUTED,
                grid=False,
                tickColor=SPOTIFY_BORDER,
                domainColor=SPOTIFY_BORDER,
            )
            .configure_legend(labelColor=SPOTIFY_MUTED, titleColor=SPOTIFY_MUTED)
        )

        st.altair_chart(ch, width="stretch")
