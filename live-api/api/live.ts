// Live train data for the dashboard. GitHub Pages can't call CTA directly: CTA sends no CORS
// headers, and an API key in a static site is public. This function holds the key and relays CTA's
// responses unmodified; Vercel's CDN caches each one for 30 s, so CTA sees one refresh per cache
// window however many people have the dashboard open.
//
// GET /api/live -> { fetched_at_ms, positions: <Locations ctatt>, arrivals: <Arrivals ctatt>[] }

const CTA_BASE = "https://lapi.transitchicago.com/api/1.0";
const ALL_ROUTES = "red,blue,brn,g,org,p,pink,y"; // one Locations call covers every 'L' line
const MAX_MAPIDS_PER_REQUEST = 4; // Arrivals API limit (errCd 105)
const TIMEOUT_MS = 8_000;

const CACHE_OK = "public, s-maxage=30, stale-while-revalidate=30";
// Errors are cached briefly so a CTA outage or exhausted quota isn't retried on every request.
const CACHE_ERROR = "public, s-maxage=10";

type Ctatt = Record<string, unknown> & { errCd?: string; errNm?: string | null };

class UpstreamError extends Error {}

/** CTA parent station IDs (mapid, 4xxxx) from a comma-separated list; null if any is invalid. */
function parseStationIds(raw: string | undefined): string[] | null {
  const ids = [...new Set((raw ?? "").split(",").map((s) => s.trim()).filter(Boolean))];
  return ids.length && ids.every((id) => /^4\d{4}$/.test(id)) ? ids : null;
}

function chunk<T>(items: T[], size: number): T[][] {
  return Array.from({ length: Math.ceil(items.length / size) }, (_, i) => items.slice(i * size, (i + 1) * size));
}

async function cta(endpoint: string, params: Record<string, string>, key: string): Promise<Ctatt> {
  const url = new URL(`${CTA_BASE}/${endpoint}`);
  url.search = new URLSearchParams({ key, ...params, outputType: "JSON" }).toString();
  let res: Response;
  try {
    res = await fetch(url, { signal: AbortSignal.timeout(TIMEOUT_MS) });
  } catch (e) {
    // Only the error's name: messages can embed the request URL, and the URL holds the key.
    throw new UpstreamError(`${endpoint}: ${e instanceof Error ? e.name : "network error"}`);
  }
  if (!res.ok) throw new UpstreamError(`${endpoint}: HTTP ${res.status}`);
  const body = (await res.json().catch(() => null)) as { ctatt?: Ctatt } | null;
  const ctatt = body?.ctatt;
  if (!ctatt || typeof ctatt !== "object") throw new UpstreamError(`${endpoint}: response has no ctatt root`);
  if (String(ctatt.errCd) !== "0") throw new UpstreamError(`${endpoint}: CTA error ${ctatt.errCd} ${ctatt.errNm ?? ""}`.trim());
  return ctatt;
}

function json(body: unknown, status: number, cacheControl: string): Response {
  // Public, read-only data with no credentials, so any origin may read it.
  return Response.json(body, { status, headers: { "Cache-Control": cacheControl, "Access-Control-Allow-Origin": "*" } });
}

export async function GET(request: Request): Promise<Response> {
  // Each distinct URL is its own CDN cache entry, so a query string would skip the cache.
  if (new URL(request.url).search) return json({ error: "This endpoint takes no query parameters." }, 400, "no-store");
  const key = process.env.CTA_API_KEY;
  const stations = parseStationIds(process.env.CTA_STATION_IDS);
  if (!key || !stations) return json({ error: "CTA_API_KEY or CTA_STATION_IDS is not configured." }, 500, "no-store");
  try {
    const [positions, ...arrivals] = await Promise.all([
      cta("ttpositions.aspx", { rt: ALL_ROUTES }, key),
      ...chunk(stations, MAX_MAPIDS_PER_REQUEST).map((ids) => cta("ttarrivals.aspx", { mapid: ids.join(",") }, key)),
    ]);
    return json({ fetched_at_ms: Date.now(), positions, arrivals }, 200, CACHE_OK);
  } catch (e) {
    if (!(e instanceof UpstreamError)) throw e;
    console.error(e.message);
    return json({ error: e.message }, 502, CACHE_ERROR);
  }
}
