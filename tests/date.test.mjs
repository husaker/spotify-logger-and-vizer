import assert from "node:assert/strict";
import test from "node:test";
import { parseLegacyPlayedAt } from "../src/google.ts";

test("existing Moscow dates and exact new ISO timestamps remain compatible", () => {
  assert.equal(parseLegacyPlayedAt("November 12, 2025 at 10:42AM"), "2025-11-12T07:42:00.000Z");
  assert.equal(parseLegacyPlayedAt("November 12, 2025 at 12:00AM"), "2025-11-11T21:00:00.000Z");
  assert.equal(parseLegacyPlayedAt("2026-11-08T08:45:12.345+03:00"), "2026-11-08T05:45:12.345Z");
  assert.equal(parseLegacyPlayedAt("invalid"), null);
});
