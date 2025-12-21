from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st
from dateutil import parser as dtparser
import gspread

from app.crypto import encrypt_str
from app.sheets_client import SheetsClient
from app.spotify_auth import build_auth_url, exchange_code_for_token, get_spotify_user_id
from app.gspread_retry import gcall
from common.config import load_settings
from worker.app_state import read_app_state, write_app_state_kv
from worker.user_sheet import ensure_user_sheet_initialized
from worker.registry import (
    REGISTRY_TAB,
    ensure_registry_headers,
    upsert_registry_user,
    find_sheet_by_spotify_user_id,
)

# -----------------------------
# Page + Spotify-ish theme
# -----------------------------
st.set_page_config(page_title="Spotify Logger", page_icon="🎧", layout="wide")

SPOTIFY_GREEN = "#1DB954"
SPOTIFY_BG = "#121212"
SPOTIFY_CARD = "#181818"
SPOTIFY_TEXT = "#FFFFFF"
SPOTIFY_MUTED = "#B3B3B3"
SPOTIFY_BORDER = "#2A2A2A"

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
</style>
""",
    unsafe_allow_html=True,
)

# -----------------------------
# Session state helpers
# -----------------------------
if "refresh_key" not in st.session_state:
    st.session_state["refresh_key"] = 0

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


def make_state() -> str:
    import secrets
    return secrets.token_urlsafe(16)


def get_query_param(name: str) -> str | None:
    # streamlit new API + fallback
    try:
        v = st.query_params.get(name)
        if isinstance(v, list):
            return v[0] if v else None
        return v
    except Exception:
        qp = st.experimental_get_query_params()
        arr = qp.get(name)
        return arr[0] if arr else None


def clear_query_params() -> None:
    try:
        st.query_params.clear()
    except Exception:
        st.experimental_set_query_params()


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


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------- Registry helpers ----------
def get_registry_ws_best_effort(*, sheets: SheetsClient, settings) -> Any | None:
    """
    Returns registry worksheet or None if unavailable.
    """
    try:
        registry_ss = sheets.open_by_key(settings.registry_sheet_id)
        registry_ws = sheets.get_or_create_worksheet(registry_ss, REGISTRY_TAB, rows=1000, cols=12)
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


# ---------- Cached log read (prevents 429 on reruns) ----------
@st.cache_data(ttl=30, show_spinner=False)
def cached_log_rows(service_json: str, sheet_id: str, refresh_key: int) -> list[list[str]]:
    sheets_local = SheetsClient.from_service_account_json(service_json)
    ss_local = sheets_local.open_by_key(sheet_id)
    ws = ss_local.worksheet("log")
    return gcall(lambda: ws.get_all_values())


def load_log_df_cached(settings, sheet_id: str) -> pd.DataFrame:
    rows = cached_log_rows(settings.google_service_account_json, sheet_id, st.session_state["refresh_key"])
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


# -----------------------------
# Header
# -----------------------------
st.markdown(
    """
<div style="display:flex; align-items:center; gap:12px;">
  <div style="font-size:40px;">🎧</div>
  <div>
    <div style="font-size:44px; font-weight:900; line-height:1;">Spotify Logger</div>
    <div style="color:#B3B3B3; margin-top:6px;">Setup & Spotify connect + Background sync + Dashboard (iteration 1)</div>
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

with st.expander("📌 Как подготовить Google Sheet (инструкция)", expanded=True):
    st.write("1) Создай Google Sheet (или используй существующий).")
    if service_email:
        st.write("2) Нажми **Share** → добавь этот email как **Editor**:")
        st.code(service_email)
    else:
        st.warning("Не смог прочитать client_email из service account JSON. Проверь GOOGLE_SERVICE_ACCOUNT_JSON.")
    st.write("3) Вставь ссылку/ID таблицы ниже.")
    st.write("4) Подключи Spotify (OAuth).")
    st.write("5) Нажми **Enable background sync** (это добавит твою sheet в общий registry для cron).")

st.divider()

# -----------------------------
# Sheet input
# -----------------------------
sheet_input = st.text_input(
    "Paste your Google Sheet URL or Sheet ID",
    placeholder="https://docs.google.com/spreadsheets/d/<ID>/edit ...",
)
sheet_id = extract_sheet_id(sheet_input)

if not sheet_id:
    st.info("Paste a Google Sheet link/ID to continue.")
    st.stop()

# Open user sheet
try:
    ss = sheets.open_by_key(sheet_id)
except Exception as e:
    st.error("❌ Can't open this Google Sheet with service account.")
    st.write("Reason:", str(e))
    st.stop()

st.success("✅ Google Sheet доступен сервис-аккаунту")

# Ensure structure exists
try:
    ensure_user_sheet_initialized(ss, timezone_name="UTC")
except Exception as e:
    st.error("❌ Не смог инициализировать листы (log/__app_state/__dedupe/cache).")
    st.write("Reason:", str(e))
    st.stop()

st.success("✅ Sheet structure OK (log/__app_state/__dedupe/cache)")

# -----------------------------
# OAuth callback handling
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
    sheet_state = read_app_state(ss)
    expected = (sheet_state.get("oauth_state") or "").strip()

    if not expected or expected != state_cb:
        st.error("OAuth state mismatch. Нажми Connect Spotify ещё раз.")
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
            st.error("Spotify не вернул refresh_token. Попробуй нажать Connect ещё раз.")
            clear_query_params()
            st.stop()

        spotify_user_id_cb = get_spotify_user_id(tokens.access_token)
        refresh_enc = encrypt_str(tokens.refresh_token, settings.fernet_key)

        write_app_state_kv(
            ss,
            {
                "spotify_user_id": spotify_user_id_cb,
                "refresh_token_enc": refresh_enc,
                "oauth_state": "",
                "last_error": "",
            },
        )

    st.success("✅ Spotify подключён! Теперь нажми Enable background sync.")
    clear_query_params()
    st.rerun()

# -----------------------------
# Read state + registry
# -----------------------------
state = read_app_state(ss)
enabled_local = (state.get("enabled") or "false").lower() == "true"
timezone_name = state.get("timezone") or "UTC"
spotify_connected = bool((state.get("refresh_token_enc") or "").strip())
spotify_user_id = (state.get("spotify_user_id") or "").strip()

registry_ws = get_registry_ws_best_effort(sheets=sheets, settings=settings)
registered, enabled_registry = (False, False)
if registry_ws is not None:
    registered, enabled_registry = registry_get_sheet_status(registry_ws, sheet_id)
background_sync_on = registered and enabled_registry

# If spotify_connected: check if this spotify_user_id already bound elsewhere
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
        if enabled_local else f'<span class="badge">Local enabled</span> <span class="badge">false</span>',
        unsafe_allow_html=True,
    )
with c2:
    st.markdown(f'<span class="badge">Timezone</span> <span class="badge">{timezone_name}</span>', unsafe_allow_html=True)
with c3:
    st.markdown(
        f'<span class="badge">Spotify</span> <span class="success-pill">connected</span>'
        if spotify_connected else f'<span class="badge">Spotify</span> <span class="badge">not connected</span>',
        unsafe_allow_html=True,
    )
with c4:
    if spotify_user_id:
        st.markdown(f'<span class="badge">Spotify user id</span> <span class="badge">{spotify_user_id}</span>', unsafe_allow_html=True)
with c5:
    st.markdown(
        f'<span class="badge">Background sync</span> <span class="success-pill">ON</span>'
        if background_sync_on else f'<span class="badge">Background sync</span> <span class="badge">OFF</span>',
        unsafe_allow_html=True,
    )

# If already has a different sheet bound — show clearly
if spotify_connected and spotify_user_id and existing_sheet_for_user and existing_sheet_for_user != sheet_id:
    st.warning(
        "⚠️ Этот Spotify аккаунт уже синхронизируется в другой таблице.\n\n"
        f"**Активная таблица:** `{existing_sheet_for_user}`\n\n"
        "Здесь включить background sync нельзя (ограничение: 1 таблица на 1 Spotify аккаунт)."
    )

# -----------------------------
# Actions
# -----------------------------
st.markdown("## Actions")

if not spotify_connected:
    st.warning("Spotify ещё не подключён. Подключи через OAuth.")

    if st.button("Connect Spotify"):
        oauth_state = make_state()
        write_app_state_kv(ss, {"oauth_state": oauth_state})

        scopes = ["user-read-recently-played", "user-read-email", "user-read-private"]
        url = build_auth_url(
            client_id=settings.spotify_client_id,
            redirect_uri=redirect_uri,
            scopes=scopes,
            state=oauth_state,
        )

        st.link_button("Open Spotify auth", url)
        st.caption(f"Redirect URI used: `{redirect_uri}` (должен быть добавлен в Spotify Dashboard)")

else:
    st.success("Spotify подключён ✅")

    col1, col2, col3, col4 = st.columns([1.3, 1.3, 1.2, 1.8])

    # Enable background sync
    with col1:
        if not background_sync_on:
            if st.button("Enable background sync"):
                if registry_ws is None:
                    st.error("Registry sheet недоступен сервис-аккаунту. Cron не сможет работать.")
                    st.stop()
                if not spotify_user_id:
                    st.error("Нет spotify_user_id в __app_state. Переподключи Spotify.")
                    st.stop()

                # Enforce one-sheet-per-user
                existing = find_sheet_by_spotify_user_id(registry_ws, spotify_user_id)
                if existing and existing != sheet_id:
                    st.error(
                        "Этот Spotify аккаунт уже подключён к другой таблице.\n\n"
                        f"**Активная таблица:** `{existing}`\n\n"
                        "Чтобы не плодить таблицы на одного юзера, background sync тут не включаю."
                    )
                    st.stop()

                # Register & enable
                upsert_registry_user(
                    registry_ws,
                    user_sheet_id=sheet_id,
                    enabled=True,
                    spotify_user_id=spotify_user_id,
                )
                write_app_state_kv(ss, {"enabled": "true"})
                st.success("Background sync enabled ✅")
                st.rerun()

    # Disable background sync
    with col2:
        if background_sync_on:
            if st.button("Disable background sync"):
                if registry_ws is None:
                    st.error("Registry sheet недоступен сервис-аккаунту.")
                    st.stop()
                upsert_registry_user(
                    registry_ws,
                    user_sheet_id=sheet_id,
                    enabled=False,
                    spotify_user_id=spotify_user_id or None,
                )
                write_app_state_kv(ss, {"enabled": "false"})
                st.info("Background sync disabled")
                st.rerun()

    # Refresh
    with col3:
        if st.button("Refresh dashboard"):
            st.session_state["refresh_key"] += 1
            st.rerun()

    with col4:
        st.caption(
            "Background sync = GitHub Actions cron.\n"
            "Включается только по кнопке.\n"
            "Dashboard читает log через cache (меньше шанс словить 429)."
        )

st.divider()

# -----------------------------
# Dashboard iteration 1
# -----------------------------
st.markdown("## Dashboard")

try:
    df = load_log_df_cached(settings, sheet_id)
except gspread.exceptions.APIError as e:
    msg = str(e)
    if "Quota exceeded" in msg or "[429]" in msg or "429" in msg:
        st.warning(
            "Google Sheets quota exceeded (429).\n\n"
            "Подожди ~60 секунд и нажми **Refresh dashboard**."
        )
        st.stop()
    raise

if df.empty:
    st.info("Пока нет данных в log. Сначала пусть воркер добавит хотя бы несколько прослушиваний.")
    st.stop()

period = st.selectbox(
    "Period",
    options=["Last 24 hours", "Last 7 days", "Last 30 days", "Last 90 days", "All time"],
    index=1,
)

now_utc = datetime.now(timezone.utc)
if period == "Last 24 hours":
    since = now_utc - timedelta(hours=24)
elif period == "Last 7 days":
    since = now_utc - timedelta(days=7)
elif period == "Last 30 days":
    since = now_utc - timedelta(days=30)
elif period == "Last 90 days":
    since = now_utc - timedelta(days=90)
else:
    since = None

df_f = df.copy()
if since is not None:
    df_f = df_f[df_f["played_at_utc"] >= pd.Timestamp(since)].copy()

# KPIs
df_24 = df[df["played_at_utc"] >= pd.Timestamp(now_utc - timedelta(hours=24))]
df_7 = df[df["played_at_utc"] >= pd.Timestamp(now_utc - timedelta(days=7))]
df_30 = df[df["played_at_utc"] >= pd.Timestamp(now_utc - timedelta(days=30))]

k1, k2, k3, k4 = st.columns([1, 1, 1, 2])
with k1:
    kpi_card("Plays • last 24h", str(len(df_24)))
with k2:
    kpi_card("Plays • last 7d", str(len(df_7)))
with k3:
    kpi_card("Plays • last 30d", str(len(df_30)))
with k4:
    kpi_card("Plays • selected period", str(len(df_f)))

st.markdown("### Top")
left, right = st.columns([1, 1])

top_artists = (
    df_f.groupby("Artist", dropna=False)
    .size()
    .reset_index(name="plays")
    .sort_values("plays", ascending=False)
    .head(15)
)

top_tracks = (
    df_f.groupby(["Track", "Artist"], dropna=False)
    .size()
    .reset_index(name="plays")
    .sort_values("plays", ascending=False)
    .head(15)
)
top_tracks["label"] = top_tracks["Track"].astype(str) + " — " + top_tracks["Artist"].astype(str)

with left:
    st.markdown("**Top artists**")
    if not top_artists.empty:
        ch = (
            alt.Chart(top_artists)
            .mark_bar(color=SPOTIFY_GREEN)
            .encode(
                x=alt.X("plays:Q", title="Plays"),
                y=alt.Y("Artist:N", sort="-x", title=None),
                tooltip=["Artist", "plays"],
            )
            .properties(height=360)
        )
        st.altair_chart(ch, use_container_width=True)
    else:
        st.info("No data for selected period.")

with right:
    st.markdown("**Top tracks**")
    if not top_tracks.empty:
        ch = (
            alt.Chart(top_tracks)
            .mark_bar(color=SPOTIFY_GREEN)
            .encode(
                x=alt.X("plays:Q", title="Plays"),
                y=alt.Y("label:N", sort="-x", title=None),
                tooltip=["Track", "Artist", "plays"],
            )
            .properties(height=360)
        )
        st.altair_chart(ch, use_container_width=True)
    else:
        st.info("No data for selected period.")

st.markdown("### Recent plays")
show_n = st.slider("Rows", min_value=20, max_value=500, value=100, step=20)

recent = df_f.head(show_n).copy()
recent["Played (UTC)"] = recent["played_at_utc"].dt.strftime("%Y-%m-%d %H:%M")
recent = recent[["Played (UTC)", "Track", "Artist", "Spotify ID", "URL"]]

st.dataframe(recent, use_container_width=True, hide_index=True)