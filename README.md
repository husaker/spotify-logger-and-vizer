# Spotify Logger and Dashboard

Spotify Logger stores Spotify listening history in Google Sheets and shows a Streamlit dashboard on top of it.

This README is only about setup and operations:

- Spotify app setup
- Google service account setup
- required Google Sheets
- local `.env` and cloud secrets
- Streamlit deployment
- GitHub Actions worker setup
- first-time user flow

Important: this project starts collecting data from the moment you connect Spotify. It is not a full historical importer.

## What You Need

- Python 3.12 recommended
- a Spotify Developer app
- a Google Cloud project with Google Sheets API enabled
- one Google service account
- one registry spreadsheet
- one user spreadsheet per Spotify account you want to track
- optional:
  - Streamlit Community Cloud for the dashboard
  - GitHub Actions for scheduled background sync

## 1. Create a Spotify Developer App

1. Go to the Spotify Developer Dashboard.
2. Create a new app.
3. Copy:
   - `Client ID`
   - `Client Secret`
4. Add redirect URIs.

Examples:

```text
http://localhost:8501
https://your-app-name.streamlit.app
```

Rules:

- `PUBLIC_APP_URL` must exactly match one of the redirect URIs in Spotify.
- The main Streamlit app uses the app URL itself as the callback.
- Do not add `/callback` for the main app.
- `SPOTIFY_REDIRECT_URI` is only for the legacy helper in `tools/spotify_connect_local.py`.

## 2. Create a Google Service Account

1. Create a Google Cloud project.
2. Enable the Google Sheets API.
3. Create a service account.
4. Download the JSON credentials file.
5. Copy the service account email from `client_email`.

You do not need a browser Google API key for this project.

## 3. Create the Required Google Sheets

### Registry Spreadsheet

Create one spreadsheet that will be used by the worker to know which user sheets are enabled for background sync.

Inside it, create a worksheet named:

```text
registry
```

Set row 1 exactly to:

```text
user_sheet_id,enabled,created_at,last_seen_at,last_sync_at,last_error,spotify_user_id
```

Share the registry spreadsheet with the service account email as `Editor`.

### User Spreadsheet

Create one normal Google Spreadsheet per Spotify account you want to track.

Share each user spreadsheet with the same service account email as `Editor`.

You do not need to create tabs manually. The app will initialize them when the sheet is first loaded.

## 4. Configure Environment Variables

The app and the worker both read the same environment variables.

Required:

- `SPOTIFY_CLIENT_ID`
- `SPOTIFY_CLIENT_SECRET`
- `PUBLIC_APP_URL`
- `REGISTRY_SHEET_ID`
- `FERNET_KEY`

Google authentication:

- local development: `GOOGLE_SERVICE_ACCOUNT_FILE`
- cloud / CI / GitHub Actions: `GOOGLE_SERVICE_ACCOUNT_JSON`

Optional tuning:

- `SYNC_LOOKBACK_MINUTES`
- `DEDUP_READ_ROWS`
- `CACHE_TTL_DAYS`

Generate a Fernet key with:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### Local `.env`

Create `.env` from `.env.example`.

Example:

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

Notes:

- For local development, `GOOGLE_SERVICE_ACCOUNT_FILE` is the easiest option.
- `PUBLIC_APP_URL` must be the exact URL you will open in the browser.
- `REGISTRY_SHEET_ID` must point to the registry spreadsheet, not a user spreadsheet.

## 5. Run the Dashboard Locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
streamlit run streamlit_app.py
```

Open the local Streamlit URL and use the app normally.

## 6. Deploy the Dashboard to Streamlit Community Cloud

Add root-level secrets in Streamlit Community Cloud:

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
- Keep the JSON valid.
- `PUBLIC_APP_URL` must match the deployed Streamlit app URL exactly.

## 7. Set Up the GitHub Actions Worker

The scheduled worker already exists at `.github/workflows/sync.yml`.

Add these GitHub repository secrets:

- `REGISTRY_SHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `FERNET_KEY`
- `SPOTIFY_CLIENT_ID`
- `SPOTIFY_CLIENT_SECRET`

Then check the following:

1. GitHub Actions is enabled for the repository.
2. The workflow file exists on the default branch.
3. You manually run the workflow once from the Actions tab using `workflow_dispatch`.
4. The manual run succeeds before you rely on the schedule.

Important:

- GitHub scheduled workflows run from the default branch.
- The current schedule is every 5 minutes.
- The worker only syncs user sheets that were enabled in the UI.

If you do not want GitHub Actions, you can run this instead from cron, a VPS, Render, Railway, or any other scheduler:

```bash
python -m worker.sync --once
```

## 8. First-Time Setup Flow for a User Sheet

1. Create a user Google Sheet.
2. Share it with the service account email as `Editor`.
3. Open the Streamlit app.
4. Paste the user sheet URL or ID into the app.
5. Click `Load sheet`.
6. Click `Connect Spotify`.
7. Complete Spotify OAuth and return to the app.
8. Click `Enable background sync` if you want scheduled syncs.
9. Run the GitHub Actions workflow once manually, or run `python -m worker.sync --once`.
10. Open the dashboard and click `Render dashboard`.

## 9. Commands You Will Probably Use

Run the dashboard locally:

```bash
streamlit run streamlit_app.py
```

Run one sync pass manually:

```bash
python -m worker.sync --once
```

Sync only one specific user sheet:

```bash
python -m worker.sync --once --sheet YOUR_USER_SHEET_ID
```

Initialize one user sheet from CLI:

```bash
python -m worker.sync --init-sheet YOUR_USER_SHEET_ID --timezone UTC
```

## 10. Setup Checklist

Before expecting data to appear, make sure all of these are true:

- Spotify redirect URI matches `PUBLIC_APP_URL`
- registry spreadsheet exists and has a `registry` worksheet
- registry header row matches exactly
- service account is an `Editor` on the registry spreadsheet
- service account is an `Editor` on the user spreadsheet
- dashboard secrets are configured
- GitHub Actions secrets are configured
- the workflow is on the default branch
- the workflow was run manually at least once
- background sync was enabled in the app for that user sheet

## Troubleshooting

### Spotify OAuth fails

- `PUBLIC_APP_URL` does not exactly match the redirect URI in Spotify.
- You are opening a different URL than the one configured in Spotify.

### The app cannot open a Google Sheet

- The spreadsheet is not shared with the service account email.
- `REGISTRY_SHEET_ID` points to the wrong spreadsheet.

### Background sync does not start

- The registry spreadsheet does not contain a worksheet named `registry`.
- The header row does not match exactly:

```text
user_sheet_id,enabled,created_at,last_seen_at,last_sync_at,last_error,spotify_user_id
```

- You did not click `Enable background sync` in the UI.
- GitHub Actions secrets are missing.
- The workflow is not on the default branch.

### GitHub Actions runs but no rows appear

- The Spotify account was connected, but background sync was never enabled for that sheet.
- The worker is running against the wrong `REGISTRY_SHEET_ID`.
- The user sheet was not shared with the service account.
- Spotify may simply not have any new recently played items since the last sync.

### Google Sheets quota errors (`429`)

- Wait around 60 seconds and try again.
- The app already caches dashboard reads, but Google Sheets still has request limits.
