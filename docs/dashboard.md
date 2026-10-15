# Phase 6: React dashboard

A static single-page app (React + TypeScript + Vite + Tailwind + Recharts + MapLibre) that reads a
JSON export of the Gold tables. There is no API server: the data is small, it only changes when
the pipeline runs, and a static site deploys free to GitHub Pages.

```
Databricks Gold ──export_dashboard_data.py──► frontend/public/data/*.json ──► React (filters run in the browser)
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
  size; the snapshot board says "static export, not live"; empty states explain what's missing.
- **Performance:** MapLibre (~1 MB) is code-split and loads after the charts; JSON exports are
  compact (about 100 KB for a day of data).
- **Basemap:** OpenFreeMap vector tiles (free, no API key), with light and dark styles.
