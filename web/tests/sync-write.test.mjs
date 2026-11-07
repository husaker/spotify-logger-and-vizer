import assert from "node:assert/strict";
import test from "node:test";
import { persistUniqueLogRows } from "../lib/server/sync-write.ts";

const row = (second, id = "track") => [`2026-11-07T12:00:${String(second).padStart(2, "0")}.123Z`, id, "artist", id, "https://example.test"];

function fixture(initial = []) {
  const log = initial.map(row => [...row]);
  const requests = [];
  const writer = {
    async ensureRows() {},
    async write(updates) {
      requests.push(structuredClone(updates));
      for (const { range, values } of updates) {
        const first = Number(range.match(/A(\d+)/)[1]) - 2;
        values.forEach((row, offset) => { log[first + offset] = [...row]; });
      }
    },
  };
  return { log, requests, writer };
}

test("dedupes the full history and current batch by timestamp, preserving genuine repeats", async () => {
  const f = fixture([row(0), row(1)]);
  assert.equal(await persistUniqueLogRows(f.log, [row(0), row(2), row(2, "other"), row(3)], f.writer), 2);
  assert.equal(f.requests[0][0].range, "log!A4:E5");
  assert.deepEqual(f.log.map(row => row[0]), [0, 1, 2, 3].map(n => row(n)[0]));
});

test("a lost write response is not blindly retried; the next run rereads the committed log", async () => {
  const f = fixture([row(0)]);
  await assert.rejects(() => persistUniqueLogRows(f.log, [row(1)], {
    ...f.writer,
    async write(updates) { await f.writer.write(updates); throw new Error("lost response"); },
  }), /lost response/);
  assert.equal(await persistUniqueLogRows(f.log, [row(1)], f.writer), 0);
  assert.equal(f.log.length, 2);
  assert.equal(f.requests.length, 1);
});

test("a write rejected before committing remains eligible on the next run", async () => {
  const f = fixture();
  await assert.rejects(() => persistUniqueLogRows(f.log, [row(1)], {
    ...f.writer, async write() { throw new Error("unavailable"); },
  }));
  assert.equal(await persistUniqueLogRows(f.log, [row(1)], f.writer), 1);
  assert.deepEqual(f.log, [row(1)]);
});

test("preserves blank historical row positions and does not write when nothing is new", async () => {
  const f = fixture([row(0), [], row(1)]);
  await persistUniqueLogRows(f.log, [row(2)], f.writer);
  assert.equal(f.requests[0][0].range, "log!A5:E5");
  assert.deepEqual(f.log[1], []);
  assert.equal(await persistUniqueLogRows(f.log, [row(2)], f.writer), 0);
  assert.equal(f.requests.length, 1);
});
