import os
import re
import sys
import time
from typing import Optional

import streamlit as st
from dotenv import load_dotenv

# Ensure project root is on sys.path so that `common`, `app`, `worker` imports work
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Load .env from project root if present
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

from common.config import AppConfig, load_config
from app.sheets_client import (
    SheetsClient,
    ensure_user_sheet_initialized,
    get_app_state,
    update_app_state,
    get_registry_client,
)
from app.spotify_auth import build_authorize_url, exchange_code_for_tokens
from app.crypto import encrypt_token
from app.date_format import now_iso_utc


SHEET_URL_RE = re.compile(r"/spreadsheets/d/([a-zA-Z0-9-_]+)")


def parse_sheet_id(sheet_input: str) -> str:
    sheet_input = sheet_input.strip()
    match = SHEET_URL_RE.search(sheet_input)
    if match:
        return match.group(1)
    # Assume it's already an ID
    if not sheet_input:
        raise ValueError("Sheet ID or URL is empty")
    return sheet_input


def get_or_init_sheets_client(config: AppConfig) -> SheetsClient:
    return SheetsClient.from_config(config)


def main() -> None:
    st.set_page_config(page_title="Spotify Track Logger", page_icon="🎧")
    st.title("Spotify Track Logger")

    # Load configuration
    try:
        config = load_config()
    except Exception as exc:  # noqa: BLE001
        st.error(f"Ошибка конфигурации: {exc}")
        st.stop()

    sheets_client = get_or_init_sheets_client(config)
    service_account_email = sheets_client.service_account_email

    st.markdown("### 1. Подготовь Google Sheet")
    st.write("1. Создай Google Sheet. 2. Поделись им с сервис-аккаунтом как **Editor**.")

    st.code(service_account_email, language="text")

    sheet_input = st.text_input("Вставь URL или ID Google Sheet", key="sheet_input")

    if not sheet_input:
        st.info("Вставь ссылку или ID таблицы, чтобы продолжить")
        st.stop()

    try:
        user_sheet_id = parse_sheet_id(sheet_input)
    except ValueError as exc:
        st.error(str(exc))
        st.stop()

    st.markdown("### 2. Проверка доступа и инициализация")

    if st.button("Check access"):
        try:
            ensure_user_sheet_initialized(sheets_client, user_sheet_id)
            st.success("Таблица подготовлена ✅")
        except Exception as exc:  # noqa: BLE001
            st.error(
                "Не удалось получить доступ к таблице. Убедись, что ты расшарил её на "
                f"service account email: {service_account_email}.\n\nПодробности: {exc}"
            )
            st.stop()

    # Always try to read app state (after user clicked anything)
    try:
        app_state = get_app_state(sheets_client, user_sheet_id)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Не удалось прочитать состояние приложения (__app_state): {exc}")
        st.stop()

    st.markdown("### 3. Статус подключения")

    enabled = app_state.get("enabled", "false").lower() == "true"
    spotify_user_id = app_state.get("spotify_user_id", "")
    refresh_token_enc = app_state.get("refresh_token_enc", "")

    if enabled and spotify_user_id and refresh_token_enc:
        st.success("У тебя уже всё подключено ✅")

        timezone = app_state.get("timezone", "UTC")
        last_synced_after_ts = app_state.get("last_synced_after_ts", "0")
        updated_at = app_state.get("updated_at", "")
        last_error = app_state.get("last_error", "")

        st.write("**Timezone:**", timezone)
        try:
            ts_ms = int(last_synced_after_ts)
            if ts_ms > 0:
                st.write(
                    "**Last synced after:**",
                    time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts_ms / 1000.0)),
                    "(UTC)",
                )
        except ValueError:
            st.write("**Last synced after:**", last_synced_after_ts)

        st.write("**Updated at:**", updated_at)
        if last_error:
            st.error(f"Last error: {last_error}")

        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("Run sync now"):
                from worker.sync import run_single_sheet_sync  # local import

                try:
                    run_single_sheet_sync(config, user_sheet_id)
                    st.success("Синхронизация завершена")
                except Exception as exc:  # noqa: BLE001
                    st.error(f"Ошибка при синхронизации: {exc}")

        with col2:
            if st.button("Disable logging"):
                app_state["enabled"] = "false"
                app_state["updated_at"] = now_iso_utc()
                update_app_state(sheets_client, user_sheet_id, app_state)
                registry = get_registry_client(sheets_client, config.registry_sheet_id)
                registry.set_enabled(user_sheet_id, False, last_error="")
                st.success("Логирование отключено")

        with col3:
            if st.button("Reconnect Spotify"):
                app_state["refresh_token_enc"] = ""
                app_state["spotify_user_id"] = ""
                app_state["updated_at"] = now_iso_utc()
                update_app_state(sheets_client, user_sheet_id, app_state)
                st.info("Теперь перепройди OAuth ниже")
    else:
        st.info("Похоже, что подключение ещё не завершено. Пройди онбординг ниже.")

    st.markdown("### 4. Подключение Spotify (OAuth)")
    st.write("1. Нажми на кнопку ниже, чтобы открыть Spotify OAuth.")
    st.write("2. После редиректа скопируй параметр `code` из URL и вставь сюда.")

    auth_url = build_authorize_url(config)
    st.link_button("Open Spotify OAuth", url=auth_url)

    auth_code = st.text_input("Вставь сюда параметр `code` из redirect URL", key="spotify_code")

    if auth_code and st.button("Complete Spotify connect"):
        try:
            token_data = exchange_code_for_tokens(config, auth_code)
        except Exception as exc:  # noqa: BLE001
            st.error(f"Ошибка при обмене кода на токен: {exc}")
            st.stop()

        refresh_token = token_data.refresh_token
        spotify_user_id = token_data.spotify_user_id

        refresh_token_enc = encrypt_token(config.fernet_key, refresh_token)

        app_state["refresh_token_enc"] = refresh_token_enc
        app_state["spotify_user_id"] = spotify_user_id
        app_state["updated_at"] = now_iso_utc()
        update_app_state(sheets_client, user_sheet_id, app_state)

        st.success("Spotify подключён ✅")

    st.markdown("### 5. Включить логирование")

    timezone = st.text_input(
        "Timezone (IANA)",
        value=app_state.get("timezone", "UTC"),
        help="Например: Europe/Moscow, Europe/Amsterdam, America/New_York",
    )

    if st.button("Enable logging"):
        if not app_state.get("refresh_token_enc") or not app_state.get("spotify_user_id"):
            st.error("Сначала подключи Spotify, чтобы мы могли получать refresh_token.")
            st.stop()

        app_state["enabled"] = "true"
        app_state["timezone"] = timezone or "UTC"
        app_state.setdefault("created_at", now_iso_utc())
        app_state["updated_at"] = now_iso_utc()
        update_app_state(sheets_client, user_sheet_id, app_state)

        registry = get_registry_client(sheets_client, config.registry_sheet_id)
        registry.register_or_update(user_sheet_id, enabled=True, last_error="")

        st.success("Логирование включено ✅")
        st.info("Воркер по cron будет автоматически дописывать новые прослушивания в log.")


if __name__ == "__main__":
    main()
