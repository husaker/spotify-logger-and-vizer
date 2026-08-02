import type { DashboardPayload } from "./types";

const catalog = [
  ["midnight", "Midnight City", "M83", "Hurry Up, We're Dreaming", "indietronica", 244000],
  ["weird", "Weird Fishes / Arpeggi", "Radiohead", "In Rainbows", "alternative rock", 318000],
  ["borderline", "Borderline", "Tame Impala", "The Slow Rush", "neo-psychedelic", 237000],
  ["mezzanine", "Teardrop", "Massive Attack", "Mezzanine", "trip hop", 330000],
  ["intro", "Intro", "The xx", "xx", "dream pop", 127000],
  ["sunset", "Sunset Lover", "Petit Biscuit", "Presence", "chillwave", 238000],
  ["everything", "Everything In Its Right Place", "Radiohead", "Kid A", "alternative rock", 251000],
  ["nightcall", "Nightcall", "Kavinsky", "OutRun", "synthwave", 258000],
] as const;

export function demoDashboard(): DashboardPayload {
  const tracks: DashboardPayload["tracks"] = {};
  const artists: DashboardPayload["artists"] = {};
  const albums: DashboardPayload["albums"] = {};
  catalog.forEach(([id, name, artist, album, genre, duration], index) => {
    const artistId = artist.toLowerCase().replaceAll(/[^a-z0-9]+/g, "-");
    const albumId = `album-${index}`;
    tracks[id] = { id, name, durationMs: duration, albumId, coverUrl: "", primaryArtistId: artistId };
    artists[artistId] = { id: artistId, name: artist, coverUrl: "", genres: [genre], primaryGenre: genre };
    albums[albumId] = { id: albumId, name: album, coverUrl: "", releaseDate: String(2007 + index * 2) };
  });
  const plays: DashboardPayload["plays"] = [];
  const now = Date.now();
  for (let day = 0; day < 365; day += 1) {
    const count = day % 11 === 0 ? 0 : 1 + ((day * 7 + 3) % 8);
    for (let index = 0; index < count; index += 1) {
      const item = catalog[(day * 3 + index * 5) % catalog.length];
      plays.push({
        playedAt: new Date(now - day * 86_400_000 - (index * 91 + day % 23) * 60_000).toISOString(),
        trackId: item[0], trackName: item[1], artistName: item[2],
        trackUrl: `https://open.spotify.com/track/${item[0]}`,
      });
    }
  }
  return {
    configured: false,
    generatedAt: new Date().toISOString(),
    timezone: "Europe/Moscow",
    stale: false,
    lastSyncAt: new Date(now - 4 * 60_000).toISOString(),
    reauthorizationRequired: false,
    plays,
    tracks,
    artists,
    albums,
  };
}

