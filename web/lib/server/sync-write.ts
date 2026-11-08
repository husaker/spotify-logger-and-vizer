import { createTimestampGuard } from "./sync-dedupe";

type LogWriter = {
  ensureRows(lastRow: number): Promise<void>;
  write(rows: Array<{ range: string; values: string[][] }>): Promise<void>;
};

// Requires the single GitHub workflow concurrency group and no other log writers.
// Read the whole log on each run, not a cursor or a separately updated dedupe sheet.
// Never retry an append on an ambiguous response; a later run rereads the log.
export async function persistUniqueLogRows(existing: string[][], candidates: string[][], writer: LogWriter): Promise<number> {
  const claim = createTimestampGuard(existing, []);
  const additions = candidates.flatMap(row => {
    if (!row[3]) return [];
    const timestamp = claim(row[0]);
    return timestamp ? [[timestamp, ...row.slice(1, 5)]] : [];
  });
  if (!additions.length) return 0;
  const first = existing.length + 2;
  const last = first + additions.length - 1;
  await writer.ensureRows(last);
  // A fixed range is idempotent if the transport repeats this exact request.
  await writer.write([{ range: `log!A${first}:E${last}`, values: additions }]);
  return additions.length;
}
