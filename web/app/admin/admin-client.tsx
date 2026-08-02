"use client";

/* eslint-disable @next/next/no-html-link-for-pages */

import { useCallback, useEffect, useState, type FormEvent } from "react";
import type { AdminStatus } from "../../lib/types";

async function api(path: string, init?: RequestInit) {
  const response = await fetch(path, { ...init, headers: { "content-type": "application/json", ...(init?.headers ?? {}) } });
  const body = await response.json().catch(() => ({})) as Record<string, unknown>;
  if (!response.ok) throw new Error(String(body.error ?? "Request failed"));
  return body;
}

function formatDate(value: string | null): string {
  return value ? new Intl.DateTimeFormat("en", { dateStyle: "medium", timeStyle: "short", timeZone: "Europe/Moscow" }).format(new Date(value)) : "Not available";
}

export default function AdminClient() {
  const [authenticated, setAuthenticated] = useState<boolean | null>(null);
  const [status, setStatus] = useState<AdminStatus | null>(null);
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState("");
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [externalUrl, setExternalUrl] = useState("");

  const load = useCallback(async () => {
    try {
      const result = await api("/api/admin/status") as unknown as AdminStatus;
      setAuthenticated(true); setStatus(result); setError("");
    } catch (reason) {
      if (reason instanceof Error && reason.message === "Unauthorized") setAuthenticated(false);
      else setError(reason instanceof Error ? reason.message : "Status unavailable");
    }
  }, []);
  useEffect(() => {
    const timer = window.setTimeout(() => { void load(); }, 0);
    return () => window.clearTimeout(timer);
  }, [load]);

  async function login(event: FormEvent) {
    event.preventDefault(); setBusy("login"); setError("");
    try { await api("/api/admin/login", { method: "POST", body: JSON.stringify({ password }) }); setPassword(""); await load(); }
    catch (reason) { setError(reason instanceof Error ? reason.message : "Login failed"); }
    finally { setBusy(""); }
  }

  async function action(name: string, path: string, body?: unknown) {
    setBusy(name); setError(""); setMessage(""); setExternalUrl("");
    try {
      const result = await api(path, { method: "POST", body: JSON.stringify(body ?? {}) });
      if (typeof result.url === "string") setExternalUrl(result.url);
      setMessage(String(result.message ?? "Done")); await load();
    } catch (reason) { setError(reason instanceof Error ? reason.message : "Action failed"); }
    finally { setBusy(""); }
  }

  if (authenticated === null) return <main className="admin-shell"><div className="admin-loading shimmer" /></main>;
  if (!authenticated) return <main className="admin-shell login-shell"><a href="/" className="back-link">← Public dashboard</a><section className="login-card"><span className="eyebrow">Spotify Logger</span><h1>Administrator access</h1><p>This page accepts only the dedicated Spotify Logger admin password. Never enter a Spotify, Google, Telegram or domain-registrar password here.</p><form onSubmit={login}><label>Spotify Logger admin password<input autoFocus required type="password" name="spotify-logger-admin-password" autoComplete="off" value={password} onChange={(event)=>setPassword(event.target.value)} /></label><button disabled={busy==="login"}>{busy==="login"?"Checking…":"Open admin"}</button></form>{error&&<div className="form-error">{error}</div>}</section></main>;

  return <main className="admin-shell">
    <header className="admin-header"><div><a href="/" className="back-link">← Public dashboard</a><span className="eyebrow">Protected operations</span><h1>Spotify Logger admin</h1><p>Connection health, manual recovery and Telegram alerts.</p></div><button className="quiet-button" onClick={()=>action("logout","/api/admin/logout")}>Log out</button></header>
    {error&&<div className="admin-notice error">{error}</div>}{message&&<div className="admin-notice success">{message}</div>}
    {externalUrl&&<div className="admin-notice success"><a className="primary-button" href={externalUrl} target="_blank" rel="noopener noreferrer">Open Telegram pairing</a></div>}
    {!status?.configured&&<div className="admin-notice error">Runtime secrets are incomplete. Follow the deployment checklist in the README before connecting services.</div>}

    <section className="admin-status-grid">
      <article><span>Spotify</span><strong className={status?.spotifyConnected&&!status.reauthorizationRequired?"ok":"warn"}>{status?.spotifyConnected&&!status.reauthorizationRequired?"Connected":"Reconnect required"}</strong><small>Authorized {formatDate(status?.authorizedAt??null)}</small></article>
      <article><span>Next reauthorization</span><strong>{formatDate(status?.reauthorizationDueAt??null)}</strong><small>Telegram reminders begin 30 days before</small></article>
      <article><span>Last sync</span><strong>{formatDate(status?.lastSyncAt??null)}</strong><small>{status?.consecutiveFailures??0} consecutive failures</small></article>
      <article><span>Telegram</span><strong className={status?.telegramConnected?"ok":"warn"}>{status?.telegramConnected?"Connected":"Not connected"}</strong><small>{status?.telegramConnected?`Since ${formatDate(status.telegramConnectedAt)}`:"Pair your private chat below"}</small></article>
    </section>

    <section className="admin-panel"><div><span className="eyebrow">Spotify authorization</span><h2>Keep collection running</h2><p>Reconnecting starts a fresh six-month authorization cycle without changing any listening history.</p></div><div className="admin-actions"><a className="primary-button" href="/api/admin/spotify/connect">{status?.spotifyConnected?"Reauthorize Spotify":"Connect Spotify"}</a><button disabled={Boolean(busy)} onClick={()=>action("sync","/api/admin/sync")}>{busy==="sync"?"Syncing…":"Run sync now"}</button></div>{status?.lastError&&<div className="safe-error"><b>Latest issue</b><span>{status.lastError}</span></div>}</section>

    <section className="admin-panel telegram-panel"><div><span className="eyebrow">Telegram alerts</span><h2>{status?.telegramConnected?"Your bot is ready":"Pair the notification bot"}</h2><p>Create the bot with <b>@BotFather</b>, save its token as <code>TELEGRAM_BOT_TOKEN</code>, then use the guided pairing button. The link expires after ten minutes.</p></div><ol><li>Open Telegram and message <b>@BotFather</b>.</li><li>Send <code>/newbot</code>, choose a name and username.</li><li>Save the supplied token as a deployment secret.</li><li>Return here and pair the chat.</li></ol><div className="admin-actions"><button className="primary-button" disabled={Boolean(busy)} onClick={()=>action("telegram","/api/admin/telegram/pair")}>{busy==="telegram"?"Preparing…":status?.telegramConnected?"Pair another chat":"Connect Telegram"}</button>{status?.telegramConnected&&<><button disabled={Boolean(busy)} onClick={()=>action("test","/api/admin/telegram/test")}>{busy==="test"?"Sending…":"Send test"}</button><button className="danger-button" disabled={Boolean(busy)} onClick={()=>action("disconnect","/api/admin/telegram/disconnect")}>Disconnect</button></>}</div></section>

    <section className="reminder-strip"><div><b>30 days</b><span>First reminder</span></div><i/><div><b>14 days</b><span>Follow-up</span></div><i/><div><b>7 days</b><span>Time to reconnect</span></div><i/><div><b>1 day</b><span>Final reminder</span></div></section>
  </main>;
}
