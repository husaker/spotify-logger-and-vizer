import assert from "node:assert/strict";
import test from "node:test";

async function worker() {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("test", `${process.pid}-${Date.now()}-${Math.random()}`);
  return (await import(workerUrl.href)).default;
}

const context = { waitUntil() {}, passThroughOnException() {} };
const env = { ASSETS: { fetch: async () => new Response("Not found", { status: 404 }) } };

test("server-renders the finished public dashboard shell", async () => {
  const response = await (await worker()).fetch(new Request("http://localhost/", { headers: { accept: "text/html" } }), env, context);
  assert.equal(response.status, 200);
  const html = await response.text();
  assert.match(html, /<title>Spotify Logger · Listening overview<\/title>/i);
  assert.doesNotMatch(html, /codex-preview|react-loading-skeleton|Your site is taking shape/i);
  assert.equal(response.headers.get("x-frame-options"), "DENY");
  assert.equal(response.headers.get("x-content-type-options"), "nosniff");
});

test("server-renders the protected admin surface without exposing secrets", async () => {
  const response = await (await worker()).fetch(new Request("http://localhost/admin", { headers: { accept: "text/html" } }), env, context);
  assert.equal(response.status, 200);
  const html = await response.text();
  assert.match(html, /<title>Admin · Spotify Logger<\/title>/i);
  assert.doesNotMatch(html, /SPOTIFY_CLIENT_SECRET|TELEGRAM_BOT_TOKEN|GOOGLE_SERVICE_ACCOUNT_JSON/);
  assert.match(response.headers.get("x-robots-tag") ?? "", /noindex/i);
});

test("API fails closed when runtime configuration is absent", async () => {
  const app = await worker();
  const dashboard = await app.fetch(new Request("http://localhost/api/dashboard"), env, context);
  assert.equal(dashboard.status, 503);
  const admin = await app.fetch(new Request("http://localhost/api/admin/status"), env, context);
  assert.equal(admin.status, 401);
});
