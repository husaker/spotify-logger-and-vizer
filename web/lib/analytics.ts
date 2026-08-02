import type { DashboardPayload, Play } from "./types";

export type RangePreset = "365" | "all" | "year" | "7" | "30" | "90" | "custom";
export type DateRange = { from: string; to: string };
export type MetricSet = { plays: number; tracks: number; artists: number; minutes: number; activeDays: number };
export type EnrichedPlay = Play & {
  day: string;
  minutes: number;
  coverUrl: string;
  albumId: string;
  albumName: string;
  artistId: string;
  artistCoverUrl: string;
  genre: string;
};

const DAY_MS = 86_400_000;

export function moscowDay(input: string | Date): string {
  const date = input instanceof Date ? input : new Date(input);
  return new Date(date.getTime() + 3 * 60 * 60 * 1000).toISOString().slice(0, 10);
}

export function todayMoscow(): string {
  return moscowDay(new Date());
}

export function shiftDay(day: string, amount: number): string {
  return new Date(Date.parse(`${day}T00:00:00Z`) + amount * DAY_MS).toISOString().slice(0, 10);
}

export function rangeForPreset(preset: RangePreset, plays: Play[], current = todayMoscow()): DateRange {
  if (preset === "all") {
    const timestamps = plays.map((play) => Date.parse(play.playedAt)).filter(Number.isFinite);
    return { from: timestamps.length ? moscowDay(new Date(Math.min(...timestamps))) : current, to: current };
  }
  if (preset === "year") return { from: `${current.slice(0, 4)}-01-01`, to: current };
  const days = preset === "7" ? 7 : preset === "30" ? 30 : preset === "90" ? 90 : 365;
  return { from: shiftDay(current, -(days - 1)), to: current };
}

export function enrich(payload: DashboardPayload): EnrichedPlay[] {
  return payload.plays.map((play) => {
    const track = payload.tracks[play.trackId];
    const artist = track ? payload.artists[track.primaryArtistId] : undefined;
    const album = track ? payload.albums[track.albumId] : undefined;
    return {
      ...play,
      day: moscowDay(play.playedAt),
      minutes: (track?.durationMs ?? 0) / 60_000,
      coverUrl: track?.coverUrl || album?.coverUrl || "",
      albumId: track?.albumId ?? "",
      albumName: album?.name ?? "Unknown album",
      artistId: track?.primaryArtistId ?? "",
      artistCoverUrl: artist?.coverUrl ?? "",
      genre: artist?.primaryGenre ?? "",
    };
  }).sort((a, b) => Date.parse(b.playedAt) - Date.parse(a.playedAt));
}

export function inRange(plays: EnrichedPlay[], range: DateRange): EnrichedPlay[] {
  return plays.filter((play) => play.day >= range.from && play.day <= range.to);
}

export function metrics(plays: EnrichedPlay[]): MetricSet {
  return {
    plays: plays.length,
    tracks: new Set(plays.map((play) => play.trackId)).size,
    artists: new Set(plays.map((play) => play.artistName)).size,
    minutes: Math.round(plays.reduce((sum, play) => sum + play.minutes, 0)),
    activeDays: new Set(plays.map((play) => play.day)).size,
  };
}

export function previousRange(range: DateRange): DateRange {
  const days = Math.round((Date.parse(`${range.to}T00:00:00Z`) - Date.parse(`${range.from}T00:00:00Z`)) / DAY_MS) + 1;
  return { from: shiftDay(range.from, -days), to: shiftDay(range.from, -1) };
}

export function delta(current: number, previous: number): number | null {
  if (previous === 0) return current === 0 ? 0 : null;
  return ((current - previous) / previous) * 100;
}

export type RankedItem = { id: string; name: string; subtitle: string; coverUrl: string; plays: number; minutes: number };
export type UniverseNode = { id: string; name: string; coverUrl: string; plays: number; minutes: number; connections: number };
export type UniverseEdge = { source: string; target: string; weight: number };
export type UniverseGraph = { nodes: UniverseNode[]; edges: UniverseEdge[] };
export type MosaicPeriod = "all" | `month:${string}` | `season:${number}:${"winter" | "spring" | "summer" | "autumn"}`;
export type MosaicPeriodOption = { value: Exclude<MosaicPeriod, "all">; label: string; kind: "season" | "month"; sortKey: string };
export type MosaicAlbum = { id: string; name: string; artistName: string; coverUrl: string; plays: number; minutes: number };

function rankBy(plays: EnrichedPlay[], key: (play: EnrichedPlay) => string, item: (play: EnrichedPlay) => Omit<RankedItem, "plays" | "minutes">): RankedItem[] {
  const map = new Map<string, RankedItem>();
  for (const play of plays) {
    const id = key(play);
    if (!id) continue;
    const current = map.get(id) ?? { ...item(play), plays: 0, minutes: 0 };
    current.plays += 1;
    current.minutes += play.minutes;
    map.set(id, current);
  }
  return [...map.values()].sort((a, b) => b.plays - a.plays || b.minutes - a.minutes).slice(0, 5);
}

export function rankings(plays: EnrichedPlay[]) {
  return {
    artists: rankBy(plays, (play) => play.artistName, (play) => ({ id: play.artistName, name: play.artistName, subtitle: "Artist", coverUrl: play.artistCoverUrl })),
    tracks: rankBy(plays, (play) => play.trackId, (play) => ({ id: play.trackId, name: play.trackName, subtitle: play.artistName, coverUrl: play.coverUrl })),
    albums: rankBy(plays, (play) => play.albumId, (play) => ({ id: play.albumId, name: play.albumName, subtitle: "Album", coverUrl: play.coverUrl })),
    genres: rankBy(plays, (play) => play.genre, (play) => ({ id: play.genre, name: play.genre, subtitle: "Genre", coverUrl: play.artistCoverUrl })),
  };
}

function artistKey(play: EnrichedPlay): string {
  return play.artistId || play.artistName;
}

export function listeningUniverse(plays: EnrichedPlay[], minimumTransitionWeight = 2): UniverseGraph {
  const maximumTransitionGapMs = 30 * 60 * 1000;
  const totals = new Map<string, UniverseNode>();
  for (const play of plays) {
    const id = artistKey(play);
    if (!id) continue;
    const current = totals.get(id) ?? {
      id,
      name: play.artistName || "Unknown artist",
      coverUrl: play.artistCoverUrl,
      plays: 0,
      minutes: 0,
      connections: 0,
    };
    current.plays += 1;
    current.minutes += play.minutes;
    if (!current.coverUrl && play.artistCoverUrl) current.coverUrl = play.artistCoverUrl;
    totals.set(id, current);
  }

  const edgeMap = new Map<string, UniverseEdge>();
  const chronological = [...plays].sort((a, b) => Date.parse(a.playedAt) - Date.parse(b.playedAt));
  for (let index = 1; index < chronological.length; index++) {
    const previous = chronological[index - 1];
    const current = chronological[index];
    const gap = Date.parse(current.playedAt) - Date.parse(previous.playedAt);
    if (!Number.isFinite(gap) || gap > maximumTransitionGapMs) continue;
    const source = artistKey(previous);
    const target = artistKey(current);
    if (!source || !target || source === target) continue;
    const [a, b] = source < target ? [source, target] : [target, source];
    const key = `${a}\u0000${b}`;
    const edge = edgeMap.get(key) ?? { source: a, target: b, weight: 0 };
    edge.weight += 1;
    edgeMap.set(key, edge);
  }

  const connections = new Map<string, number>();
  const edges = [...edgeMap.values()]
    .filter((edge) => edge.weight >= minimumTransitionWeight)
    .sort((a, b) => b.weight - a.weight);
  const visible = new Set(edges.flatMap((edge) => [edge.source, edge.target]));
  for (const edge of edges) {
    connections.set(edge.source, (connections.get(edge.source) ?? 0) + edge.weight);
    connections.set(edge.target, (connections.get(edge.target) ?? 0) + edge.weight);
  }
  const nodes = [...totals.values()]
    .filter((node) => visible.has(node.id))
    .sort((a, b) => b.minutes - a.minutes || b.plays - a.plays || a.name.localeCompare(b.name));
  for (const node of nodes) node.connections = connections.get(node.id) ?? 0;

  return { nodes, edges };
}

function seasonForDay(day: string): { value: MosaicPeriodOption["value"]; label: string; sortKey: string } {
  const year = Number(day.slice(0, 4));
  const month = Number(day.slice(5, 7));
  if (month === 12 || month <= 2) {
    const startYear = month === 12 ? year : year - 1;
    return { value: `season:${startYear}:winter`, label: `Winter ${startYear}–${String(startYear + 1).slice(-2)}`, sortKey: `${startYear}-12-01` };
  }
  const season = month <= 5 ? "spring" : month <= 8 ? "summer" : "autumn";
  const startMonth = season === "spring" ? "03" : season === "summer" ? "06" : "09";
  return { value: `season:${year}:${season}`, label: `${season[0].toUpperCase()}${season.slice(1)} ${year}`, sortKey: `${year}-${startMonth}-01` };
}

export function mosaicPeriodOptions(plays: EnrichedPlay[]): MosaicPeriodOption[] {
  const seasons = new Map<string, MosaicPeriodOption>();
  const months = new Map<string, MosaicPeriodOption>();
  for (const play of plays) {
    const season = seasonForDay(play.day);
    seasons.set(season.value, { ...season, kind: "season" });
    const month = play.day.slice(0, 7);
    months.set(`month:${month}`, {
      value: `month:${month}`,
      label: new Intl.DateTimeFormat("en", { month: "long", year: "numeric", timeZone: "UTC" }).format(new Date(`${month}-01T00:00:00Z`)),
      kind: "month",
      sortKey: `${month}-01`,
    });
  }
  const newestFirst = (a: MosaicPeriodOption, b: MosaicPeriodOption) => b.sortKey.localeCompare(a.sortKey);
  return [...seasons.values()].sort(newestFirst).concat([...months.values()].sort(newestFirst));
}

function inMosaicPeriod(play: EnrichedPlay, period: MosaicPeriod): boolean {
  if (period === "all") return true;
  if (period.startsWith("month:")) return play.day.slice(0, 7) === period.slice(6);
  return seasonForDay(play.day).value === period;
}

export function albumMosaic(plays: EnrichedPlay[], period: MosaicPeriod = "all", limit = 48): MosaicAlbum[] {
  const albums = new Map<string, MosaicAlbum>();
  for (const play of plays) {
    if (!inMosaicPeriod(play, period)) continue;
    const id = play.albumId || `album:${play.albumName}`;
    const current = albums.get(id) ?? {
      id,
      name: play.albumName || "Unknown album",
      artistName: play.artistName || "Unknown artist",
      coverUrl: play.coverUrl,
      plays: 0,
      minutes: 0,
    };
    current.plays += 1;
    current.minutes += play.minutes;
    if (!current.coverUrl && play.coverUrl) current.coverUrl = play.coverUrl;
    albums.set(id, current);
  }
  return [...albums.values()]
    .sort((a, b) => b.minutes - a.minutes || b.plays - a.plays || a.name.localeCompare(b.name))
    .slice(0, Math.max(1, limit));
}

export function activity(plays: EnrichedPlay[], today = todayMoscow()): Array<{ day: string; plays: number; level: number }> {
  const counts = new Map<string, number>();
  for (const play of plays) counts.set(play.day, (counts.get(play.day) ?? 0) + 1);
  const values = [...counts.values()].sort((a, b) => a - b);
  return Array.from({ length: 365 }, (_, index) => {
    const day = shiftDay(today, index - 364);
    const count = counts.get(day) ?? 0;
    const rank = count ? values.filter((value) => value <= count).length / Math.max(1, values.length) : 0;
    return { day, plays: count, level: count === 0 ? 0 : Math.max(1, Math.ceil(rank * 4)) };
  });
}

export function activityStats(data: ReturnType<typeof activity>) {
  let longestStreak = 0;
  let currentStreak = 0;
  let run = 0;
  let busiest = data[0] ?? { day: todayMoscow(), plays: 0, level: 0 };
  for (const item of data) {
    run = item.plays > 0 ? run + 1 : 0;
    longestStreak = Math.max(longestStreak, run);
    if (item.plays > busiest.plays) busiest = item;
  }
  for (let index = data.length - 1; index >= 0 && data[index].plays > 0; index--) currentStreak += 1;
  return {
    plays: data.reduce((sum, item) => sum + item.plays, 0),
    activeDays: data.filter((item) => item.plays > 0).length,
    longestStreak,
    currentStreak,
    busiestDay: busiest.day,
    busiestPlays: busiest.plays,
  };
}

function bucketKey(day: string, monthly: boolean): string {
  if (monthly) return day.slice(0, 7);
  const date = new Date(`${day}T00:00:00Z`);
  const weekday = date.getUTCDay() || 7;
  return shiftDay(day, -(weekday - 1));
}

function nextBucket(key: string, monthly: boolean): string {
  if (!monthly) return shiftDay(key, 7);
  const [year, month] = key.split("-").map(Number);
  return new Date(Date.UTC(year, month, 1)).toISOString().slice(0, 7);
}

function bucketKeys(range: DateRange, monthly: boolean): string[] {
  const first = bucketKey(range.from, monthly);
  const last = bucketKey(range.to, monthly);
  if (!first || !last || first > last) return [];
  const keys: string[] = [];
  for (let key = first; key <= last; key = nextBucket(key, monthly)) keys.push(key);
  return keys;
}

export function weeklySeries(plays: EnrichedPlay[], range: DateRange, monthly = false): Array<{ label: string; value: number; minutes: number }> {
  const buckets = new Map<string, { value: number; minutes: number }>();
  for (const key of bucketKeys(range, monthly)) buckets.set(key, { value: 0, minutes: 0 });
  for (const play of plays) {
    const key = bucketKey(play.day, monthly);
    const bucket = buckets.get(key) ?? { value: 0, minutes: 0 };
    bucket.value += 1;
    bucket.minutes += play.minutes;
    buckets.set(key, bucket);
  }
  return [...buckets.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([label, value]) => ({ label, ...value }));
}

export function discoverySeries(selected: EnrichedPlay[], all: EnrichedPlay[], range: DateRange, monthly = false): Array<{ label: string; fresh: number; replay: number; score: number }> {
  const first = new Map<string, string>();
  for (const play of [...all].reverse()) if (!first.has(play.trackId) || play.day < first.get(play.trackId)!) first.set(play.trackId, play.day);
  const buckets = new Map<string, { fresh: Set<string>; all: Set<string> }>();
  for (const key of bucketKeys(range, monthly)) buckets.set(key, { fresh: new Set(), all: new Set() });
  for (const play of selected) {
    const key = bucketKey(play.day, monthly);
    const bucket = buckets.get(key) ?? { fresh: new Set(), all: new Set() };
    bucket.all.add(play.trackId);
    const firstDay = first.get(play.trackId)!;
    const firstKey = bucketKey(firstDay, monthly);
    if (firstKey === key) bucket.fresh.add(play.trackId);
    buckets.set(key, bucket);
  }
  return [...buckets.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([label, bucket]) => ({
    label,
    fresh: bucket.fresh.size,
    replay: bucket.all.size - bucket.fresh.size,
    score: bucket.all.size ? bucket.fresh.size / bucket.all.size : 0,
  }));
}

export function fingerprint(plays: EnrichedPlay[], metric: "plays" | "minutes"): number[][] {
  const grid = Array.from({ length: 7 }, () => Array.from({ length: 24 }, () => 0));
  for (const play of plays) {
    const date = new Date(Date.parse(play.playedAt) + 3 * 60 * 60 * 1000);
    const day = (date.getUTCDay() + 6) % 7;
    grid[day][date.getUTCHours()] += metric === "plays" ? 1 : play.minutes;
  }
  return grid;
}
