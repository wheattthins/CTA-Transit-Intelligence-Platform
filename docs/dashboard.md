# Phase 6: React dashboard

A static single-page app (React + TypeScript + Vite + Tailwind + Recharts + MapLibre) that reads a
JSON export of the Gold tables. The analytics need no API server: the data is small, it only
changes when the pipeline runs, and a static site deploys free to GitHub Pages. The one exception
is the map and next-trains board, which show live train data through a small Vercel function
(see [Live map and board](#live-map-and-board)).

```
Databricks Gold ──export_dashboard_data.py──► frontend/public/data/*.json ──► React (filters run in the browser)
CTA Train Tracker ──live-api (Vercel, 30 s CDN cache)──────────────────────► map + next-trains board
```

## Run it

```powershell
# 1. Export data (pick one)
python databricks/export_dashboard_data.py          # from Databricks (SQL warehouse)
python databricks/export_dashboard_data.py --local  # run the pipeline locally on data/raw (needs Java)

# 2. Start the dashboard
cd frontend
npm install
npm run dev          # http://localhost:5173
npm test             # aggregation and formatting tests
npm run build        # type-check + production build into frontend/dist
```

Map layers (`frontend/public/data/geo/`) come from the City of Chicago Data Portal and are
committed. Rebuild them with `python scripts/build_map_geodata.py`.

## Publishing (GitHub Pages)
1. Repository **Settings → Pages → Source: GitHub Actions** (one time).
2. Commit a fresh export (`frontend/public/data`) and push. `.github/workflows/dashboard.yml` tests,
   builds and deploys to `https://<user>.github.io/<repo>/`.

## Live map and board
The browser can't call CTA itself: CTA sends no CORS headers, and an API key in a static site is
public. `live-api/api/live.ts` is a Vercel function that holds the key and returns, in one
response, the Locations API (every train on all 8 lines) and the Arrivals API for the monitored
stations (4 stations per call, CTA's limit). It relays CTA's JSON unmodified, and
`frontend/src/lib/live.ts` parses it into the same shapes as the export.

- **Caching:** responses carry `s-maxage=30, stale-while-revalidate=30`, so Vercel's CDN calls CTA
  at most about once per 30 s per CDN region, however many people are watching. Requests with a
  query string are rejected, so they can't be used to skip the cache. CTA errors are cached for 10 s.
- **Polling:** the dashboard polls every 30 s, only while the tab is visible.
- **Fallback:** until the feed answers, or if it was never configured, the map and board show the
  exported snapshot and say so. If a later poll fails, they keep the last live data and show
  "Reconnecting" with its time.
- **Filters:** live positions cover every train, so only the line filter applies on the map; the
  station filter zooms to the station. The board applies both.

Deploy: the Vercel project `cta-transit-live` is connected to this GitHub repo with **Root Directory
`live-api`**, so every push to `main` deploys the function (other branches get preview deployments,
which have no CTA key). Without the Root Directory setting, Vercel builds the repo root, finds no
function, and every URL returns 404. To redeploy without a push, use **Redeploy** on the latest
deployment in the Vercel dashboard. The two environment variables are set once (Production only):
```powershell
cd live-api
npx vercel env add CTA_API_KEY production --sensitive   # paste the key when prompted
npx vercel env add CTA_STATION_IDS production           # the monitored stations, e.g. 41320,40380,40900
```
`dashboard.yml` builds with `VITE_LIVE_API_URL=https://cta-transit-live.vercel.app/api/live`.
For live data in local dev, run `$env:VITE_LIVE_API_URL="https://cta-transit-live.vercel.app/api/live"; npm run dev`.

**Quota:** the function shares CTA's daily request limit with the collector when they use the same
key. Each refresh is 1 + ⌈stations ÷ 4⌉ calls, so 9 stations cost 4 calls per 30 s, about 11,500 a
day per CDN region if someone keeps the dashboard open around the clock. A second Train Tracker
key for the function keeps a busy dashboard from ever using up the collector's limit.

## How filtering stays exact
The dashboard never averages averages. The daily Gold tables (`daily_line_metrics`,
`daily_accuracy_by_horizon`) store **counts and sums** per service day, station, line, day type
and time period. For any filter the browser sums the rows, then divides:

| Metric | Computed as |
|---|---|
| Avg prediction error | Σ abs_error_sum ÷ Σ scored_predictions |
| Within 1 min | Σ within_1_min ÷ Σ scored_predictions |
| Avg headway | Σ headway_sum ÷ Σ reliable_headways |
| Headway stddev / CV | √((Σx² − (Σx)²/n) / (n−1)), ÷ mean |

`databricks/tests/test_gold.py` proves that summing daily rows reproduces the rolled-up Gold
metrics, and `frontend/src/test/metrics.test.ts` checks the browser-side math.

## Design decisions
- **Line colors** keep CTA's hues (riders read Red/Brown/Purple instantly) but are re-stepped in
  lightness per theme, so every pair passes a palette validator: lightness band, chroma floor,
  colorblind separation (protan/deutan/tritan) and contrast. Official CTA Brown and Purple are too
  dark for chart marks, and official Red vs Brown are indistinguishable to deuteranopes at equal
  lightness. The validated steps live in `src/index.css`.
- **Rush vs non-rush** uses two steps of one hue (an ordered comparison), not two line colors.
- **Accessibility:** every chart has a Chart | Table toggle (no value is tooltip-only); legends
  always accompany color; status badges are icon + label; keyboard-operable segmented controls;
  light and dark themes, each with its own validated steps; `prefers-reduced-motion` respected.
- **Honesty:** inferred arrivals are labeled as inferred everywhere; each figure shows its sample
  size; the map and board say whether they're live (with the data's time) or showing the static
  export; empty states explain what's missing.
- **Performance:** MapLibre (~1 MB) is code-split and loads after the charts; JSON exports are
  compact (about 100 KB for a day of data).
- **Basemap:** OpenFreeMap vector tiles (free, no API key), with light and dark styles.
