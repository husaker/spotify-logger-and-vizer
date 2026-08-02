# Spotify Logger

Spotify Logger is a public listening dashboard with a protected administration area and a five-minute Spotify collection job. The application is implemented in React and TypeScript and is hosted with OpenAI Sites. Google Sheets remains the source of truth for listening history and application state.

The active application lives in [`web/`](web/README.md). The former Streamlit application, Python collector, multi-user registry, and GitHub Actions scheduler were retired after the full-stack migration.

## Repository layout

- `web/app/` — dashboard and admin interface
- `web/lib/` — analytics and server integrations
- `web/worker/` — HTTP API and scheduled synchronization entry point
- `web/tests/` — analytics, rendering, and security tests
- `web/.openai/hosting.json` — Sites hosting configuration

## Development

```bash
cd web
npm install
npm run dev
```

See [`web/README.md`](web/README.md) for required secrets, Spotify callback configuration, Google Sheet requirements, Telegram pairing, validation, and deployment notes.
