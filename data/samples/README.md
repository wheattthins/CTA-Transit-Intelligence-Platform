# Sample data — NOT live CTA information

Files prefixed `SAMPLE_` are **hand-written fixtures** shaped like real CTA Train Tracker
API responses. They exist so the collector and tests can run without an API key.

- Station and stop IDs for Belmont (41320; stops 30255–30258) match the official
  [CTA 'L' stops list](https://data.cityofchicago.org/Transportation/CTA-System-Information-List-of-L-Stops/8pix-ypme).
- Run numbers, times, coordinates and flags are **invented**.
- `python -m backend.collectors.train_tracker --sample` uses this data. Its output is labeled
  `SAMPLE DATA` and written to `data/sample_runs/` (never `data/raw/`), with
  `"is_sample": true` in the metadata.

Do not use these files for analytics.
