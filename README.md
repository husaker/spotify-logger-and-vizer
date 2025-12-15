### Spotify Track Logger

Простое production-ready приложение для логирования прослушиваний Spotify в Google Sheets.

### Что делает

- **Пользователь**:
  - **создаёт Google Sheet** и даёт доступ сервис-аккаунту (Editor),
  - **вставляет URL/ID таблицы** в UI Streamlit,
  - **подключает Spotify (OAuth)**,
  - **включает логирование**.
- **Воркер** по cron (`python -m worker.sync --once`) читает реестр и дописывает новые прослушивания в `log` вкладку Google Sheet пользователя.

### Структура данных в Google Sheet пользователя

- **Вкладка `log` (основной лог)** — ровно 5 колонок:
  - `Date | Track | Artist | Spotify ID | URL`
- **Формат `Date`**:
  - `{Month} {day}, {year} at {hour}:{minute:02d}{AMPM}`
  - пример: `November 12, 2025 at 10:42AM`
- **Artist**: список артистов через запятую `,`.
- **Spotify ID**: `track_id`.
- **URL**: `https://open.spotify.com/track/<track_id>`.
- **Служебные данные** хранятся только в скрытых вкладках, не в `log`.

### Служебные вкладки в user sheet

- `__app_state` — key/value состояние приложения.
- `__dedupe` — таблица dedupe-ключей.
- `__cache_tracks` — кэш по трекам.
- `__cache_artists` — кэш по артистам.
- `__cache_albums` — кэш по альбомам.

Форматы вкладок см. в коде `app/sheets_client.py` и `worker/cache.py`.

### Registry sheet

Отдельная Google Sheet с одной вкладкой `registry`:

- **Колонки**: `user_sheet_id, enabled, created_at, last_seen_at, last_sync_at, last_error`.
- Воркер читает список пользователей из этой таблицы и синкает только `enabled=true`.

### ENV переменные

**Обязательные**:

- `SPOTIFY_CLIENT_ID`
- `SPOTIFY_CLIENT_SECRET`
- `SPOTIFY_REDIRECT_URI`
- `GOOGLE_SERVICE_ACCOUNT_JSON` **или** `GOOGLE_SERVICE_ACCOUNT_FILE`
- `REGISTRY_SHEET_ID`
- `FERNET_KEY`

**Опциональные (с дефолтами)**:

- `SYNC_LOOKBACK_MINUTES` (default `120`)
- `DEDUP_READ_ROWS` (default `5000`)
- `CACHE_TTL_DAYS` (default `30`)
- `SYNC_PAGE_LIMIT` (default `50`)
- `MAX_PAGES_PER_RUN` (default `10`)

Смотри пример `.env.example`.

### Установка

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
cp .env.example .env
# отредактируй .env
```

### Настройка Spotify App

1. Зайди в [Spotify Developer Dashboard](https://developer.spotify.com/dashboard/) и создай приложение.
2. В настройках приложения укажи Redirect URI из `SPOTIFY_REDIRECT_URI` (например `http://localhost:8501`).
3. Скопируй `Client ID` и `Client Secret` в `.env` (`SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET`).

### Настройка Google Service Account + Sheets API

1. Создай сервис-аккаунт в Google Cloud Console.
2. Включи Google Sheets API и Google Drive API для этого проекта.
3. Создай JSON key, скачай файл.
4. Либо:
   - положи путь к файлу в `GOOGLE_SERVICE_ACCOUNT_FILE`,
   - либо вставь JSON как строку в `GOOGLE_SERVICE_ACCOUNT_JSON`.
5. Скопируй email сервис-аккаунта — его нужно будет дать пользователям.

### Создание registry sheet

1. Создай новую Google Sheet, переименуй первую вкладку в `registry`.
2. В первой строке укажи хедеры: `user_sheet_id, enabled, created_at, last_seen_at, last_sync_at, last_error`.
3. Возьми ID таблицы из URL (`https://docs.google.com/spreadsheets/d/<ID>/...`) и вставь в `.env` как `REGISTRY_SHEET_ID`.
4. Поделись этой таблицей с сервис-аккаунтом (Editor).

### Шаги пользователя (user sheet)

1. Создать свою Google Sheet.
2. Поделиться ею с email сервис-аккаунта (Editor).
3. Открыть Streamlit UI.
4. Вставить URL/ID своей таблицы.
5. Нажать `Check access` — приложение проинициализирует все вкладки и заголовки.
6. Пройти Spotify OAuth, вставить `code` в UI.
7. Нажать `Enable logging`.

### Локальный запуск Streamlit

```bash
streamlit run app/streamlit_app.py
```

### Локальный запуск воркера

- **Один раз по всему реестру**:

```bash
python -m worker.sync --once
```

- **Синк только одной таблицы**:

```bash
python -m worker.sync --sheet <SHEET_ID> --once
```

### Пример GitHub Actions cron workflow

```yaml
name: spotify-track-logger-cron

on:
  schedule:
    - cron: "*/15 * * * *"  # каждые 15 минут

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -r requirements.txt
      - run: python -m worker.sync --once
        env:
          SPOTIFY_CLIENT_ID: ${{ secrets.SPOTIFY_CLIENT_ID }}
          SPOTIFY_CLIENT_SECRET: ${{ secrets.SPOTIFY_CLIENT_SECRET }}
          SPOTIFY_REDIRECT_URI: ${{ secrets.SPOTIFY_REDIRECT_URI }}
          GOOGLE_SERVICE_ACCOUNT_JSON: ${{ secrets.GOOGLE_SERVICE_ACCOUNT_JSON }}
          REGISTRY_SHEET_ID: ${{ secrets.REGISTRY_SHEET_ID }}
          FERNET_KEY: ${{ secrets.FERNET_KEY }}
          SYNC_LOOKBACK_MINUTES: 120
          DEDUP_READ_ROWS: 5000
          CACHE_TTL_DAYS: 30
          SYNC_PAGE_LIMIT: 50
          MAX_PAGES_PER_RUN: 10
```

### Пример деплоя в Cloud Run + Scheduler (high-level)

1. Собрать Docker-образ с приложением (Streamlit + worker).
2. Задеплоить Streamlit-сервис в Cloud Run (порт 8501), используя переменные окружения.
3. Задеплоить отдельный Cloud Run сервис для воркера, чья команда по умолчанию — `python -m worker.sync --once`.
4. В Cloud Scheduler создать задачу HTTP, которая раз в N минут дергает воркер-сервис.
5. Все секреты (Spotify, Google, Fernet) хранить в Secret Manager и прокидывать в Cloud Run как env.

### Запуск команд

- `streamlit run app/streamlit_app.py`
- `python -m worker.sync --once`
- `python -m worker.sync --sheet <SHEET_ID> --once`
