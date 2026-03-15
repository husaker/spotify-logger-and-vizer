# Spotify Logger and Dashboard

Spotify Logger and Dashboard is a DB-less Spotify analytics app built with Streamlit, Google Sheets, and the Spotify Web API.

It lets you connect a Spotify account, store listening history in your own Google Sheet, and explore it through a dashboard with KPIs, top artists/tracks/albums, genres, listening fingerprint charts, and new-vs-repeat analysis.

## How it works

- Streamlit is the UI for setup and analytics.
- Google Sheets is the storage layer.
- Spotify provides recently played tracks and metadata.
- A background worker syncs new plays into each user sheet.
- GitHub Actions can run that worker every 5 minutes.

Each user keeps their own Google Sheet. The app creates tabs such as `log`, `__app_state`, `__dedupe`, and metadata caches inside that sheet.

This project starts collecting data from the moment you connect it. It is not a full historical Spotify importer.

## Features

- Connect Spotify with OAuth
- Store listening events in your own Google Sheet
- Run without a database
- Enrich tracks, artists, and albums with cached metadata
- View a Streamlit dashboard with:
  - total plays
  - unique tracks and artists
  - minutes listened
  - activity grid
  - top 5 artists, tracks, albums, and genres
  - weekly listening average
  - listening fingerprint by weekday and hour
  - new vs repeat listening analysis

## Requirements

- Python 3.12 recommended
- A Spotify Developer application
- A Google Cloud service account with Google Sheets API enabled
- Two Google Sheets:
  - one registry spreadsheet for background sync bookkeeping
  - one or more user spreadsheets for actual listening logs

## 1. Create a Spotify Developer app

1. Go to the Spotify Developer Dashboard.
2. Create a new app.
3. Copy:
   - `Client ID`
   - `Client Secret`
4. Add redirect URIs.

For local development, the main app expects the Streamlit app URL itself as the callback URL, for example:

```text
http://localhost:8501
```

If you deploy the dashboard, add your production URL too, for example:

```text
https://your-app-name.streamlit.app
```

Important:

- `PUBLIC_APP_URL` must exactly match one of the redirect URIs configured in Spotify.
- The main Streamlit app does not use a `/callback` path.
- `SPOTIFY_REDIRECT_URI` is only needed for the legacy local helper in `tools/spotify_connect_local.py`.

## 2. Create a Google Cloud service account

1. Create a Google Cloud project.
2. Enable the Google Sheets API.
3. Create a service account.
4. Download its JSON credentials file.
5. Find the service account email in the JSON file under `client_email`.

The app uses this service account to read and write Google Sheets.

You do not need a browser-style Google API key for this project. Use a Google service account JSON credential instead.

## 3. Create the required Google Sheets

### Registry spreadsheet

Create one Google Spreadsheet that will act as the global registry for background sync.

Inside it, create a worksheet named:

```text
registry
```

Set the first row exactly to:

```text
user_sheet_id,enabled,created_at,last_seen_at,last_sync_at,last_error,spotify_user_id
```

Share the registry spreadsheet with the service account email as `Editor`.

### User spreadsheet

Create a normal Google Spreadsheet for each user/account you want to track.

Share each user spreadsheet with the same service account email as `Editor`.

You do not need to create tabs manually. The app will initialize them when the sheet is first loaded.

## 4. Configure environment variables

### Local `.env`

Create `.env` from `.env.example`.

You can use either:

- `GOOGLE_SERVICE_ACCOUNT_FILE` for local development
- `GOOGLE_SERVICE_ACCOUNT_JSON` for CI / cloud deployments

Generate a Fernet key with:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Example local `.env`:

```env
# Spotify
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
PUBLIC_APP_URL=http://localhost:8501

# Optional: only for tools/spotify_connect_local.py
SPOTIFY_REDIRECT_URI=http://127.0.0.1:8765/callback

# Google Sheets
REGISTRY_SHEET_ID=your_registry_spreadsheet_id
GOOGLE_SERVICE_ACCOUNT_FILE=secrets/service_account.json
GOOGLE_SERVICE_ACCOUNT_JSON=

# Security
FERNET_KEY=your_generated_fernet_key

# Worker tuning
SYNC_LOOKBACK_MINUTES=120
DEDUP_READ_ROWS=5000
CACHE_TTL_DAYS=30
```

### Streamlit Community Cloud secrets

If you deploy the dashboard to Streamlit Community Cloud, add the same values in the app secrets at the root level.

This app reads environment variables, so your deployment must expose these values to the running process.

Example:

```toml
SPOTIFY_CLIENT_ID = "your_spotify_client_id"
SPOTIFY_CLIENT_SECRET = "your_spotify_client_secret"
PUBLIC_APP_URL = "https://your-app-name.streamlit.app"

REGISTRY_SHEET_ID = "your_registry_spreadsheet_id"
FERNET_KEY = "your_generated_fernet_key"

GOOGLE_SERVICE_ACCOUNT_JSON = """{"type":"service_account","project_id":"...","private_key_id":"...","private_key":"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n","client_email":"...","client_id":"...","token_uri":"https://oauth2.googleapis.com/token"}"""
```

Notes:

- For Streamlit Cloud, prefer `GOOGLE_SERVICE_ACCOUNT_JSON`.
- Do not use `GOOGLE_SERVICE_ACCOUNT_FILE` there.
- Keep the JSON valid. A single-line JSON string or a triple-quoted JSON string both work well here.

## 5. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## 6. Run the dashboard locally

```bash
streamlit run streamlit_app.py
```

Open the local Streamlit URL, then:

1. Paste a Google Sheet URL or sheet ID for a user sheet.
2. Click `Load sheet`.
3. Connect Spotify.
4. Optionally enable background sync.
5. Choose a date range and click `Render dashboard`.

## 7. Run the sync worker

Run one sync pass manually:

```bash
python -m worker.sync --once
```

Sync only one specific sheet:

```bash
python -m worker.sync --once --sheet YOUR_USER_SHEET_ID
```

Initialize one user sheet from the CLI:

```bash
python -m worker.sync --init-sheet YOUR_USER_SHEET_ID --timezone UTC
```

What the worker does:

- reads enabled sheets from the registry
- refreshes Spotify access tokens
- fetches recently played tracks
- appends new rows to `log`
- updates dedupe keys
- enriches cache tabs with track, artist, and album metadata

## 8. Enable background sync with GitHub Actions

The repository already contains a scheduled workflow at `.github/workflows/sync.yml`.

Add these GitHub repository secrets:

- `REGISTRY_SHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `FERNET_KEY`
- `SPOTIFY_CLIENT_ID`
- `SPOTIFY_CLIENT_SECRET`

Then the worker can run on schedule. The current workflow is configured for every 5 minutes.

If you prefer, you can also run `python -m worker.sync --once` from any other scheduler such as cron, Railway, Render, or a VPS.

## 9. First-time setup flow for a new user

1. Start the Streamlit app.
2. Create a user Google Sheet.
3. Share that sheet with the service account email.
4. Paste the sheet URL into the app and click `Load sheet`.
5. Click `Connect Spotify`.
6. Return to the app after Spotify authorizes the account.
7. Click `Enable background sync` if you want scheduled syncs.
8. Run the worker once manually, or wait for the next scheduled GitHub Actions run.
9. Open the dashboard and render a date range.

## Troubleshooting

### Spotify OAuth fails

- Double-check that `PUBLIC_APP_URL` exactly matches a redirect URI in your Spotify app settings.
- Make sure you are opening the same URL that you configured in Spotify.

### The app cannot open a Google Sheet

- Make sure the spreadsheet is shared with the service account email as `Editor`.
- Make sure `REGISTRY_SHEET_ID` points to the registry spreadsheet, not a user spreadsheet.

### Background sync does not start

- Make sure the registry spreadsheet has a worksheet named `registry`.
- Make sure the header row matches exactly:

```text
user_sheet_id,enabled,created_at,last_seen_at,last_sync_at,last_error,spotify_user_id
```

- Make sure you clicked `Enable background sync` in the UI.

### Google Sheets quota errors (`429`)

- Wait around 60 seconds and try again.
- The app already caches dashboard reads, but Google Sheets still has request limits.
