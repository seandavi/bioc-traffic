# Access-log analytics: what exists and how to query it

Operational companion to the published download-stats page. **That page is public; this repo is not** —
it carries internal paths, account-specific URIs and secret names, none of which belong on the
published site. See [ADR 0001](https://github.com/seandavi/bioc-infrastructure/blob/main/adr/0001-public-docs-site-with-a-publication-boundary.md).

Design rationale lives in the ADRs: [0002](https://github.com/seandavi/bioc-infrastructure/blob/main/adr/0002-mirror-access-logs-unfiltered.md)
(mirror unfiltered, interpret with views), [0004](https://github.com/seandavi/bioc-infrastructure/blob/main/adr/0004-download-statistics-are-generated-static-files.md)
(databases are build-time infrastructure, not serving infrastructure).

## What exists

Sizes as of 2026-10-01. Everything here is private: it carries `c_ip` or `client_id`.

| Artifact | Location | Size | Notes |
|---|---|---|---|
| Raw CloudFront logs | `/data/davsean/bioc-cf-logs-raw` (from S3 `aws-bioc:bioc-cloudfront-logs`) | 599 GB | system of record; 2020-01-01 → the cutover tail (#15) |
| CloudFront Parquet (local) | `/data/davsean/bioc-cf-parquet` | 448 GB, 81 files | `year=/month=`, one file per month, through 2026-09 |
| CloudFront Parquet (R2) | `r2:bioc-cloudfront-logs/parquet/` | same 81 files | 2026-08/09 re-uploaded 2026-09-30, `rclone check` clean |
| Iceberg table | `biocr2.cloudfront.access_logs` | metadata only | **stale for 2026-08** (file replaced under it, #15) |
| Raw Logpush records | `gs://bioc-u24-logs/cloudflare/bioc-access-logs/{DATE}/` | ~16 GB/day decompressed | system of record for the Cloudflare era (below) |
| Cloudflare Parquet (local) | `/data/davsean/bioc-cloudflare-parquet` | 2.2 GB | `year=/month=/day=/hour=`, one file per UTC hour, from 2026-08-06 |
| Cloudflare Parquet (R2) | `r2:bioc-access-logs/parquet/cloudflare/` | same | canonical copy of the derived Parquet |
| Package download stats | `/data/davsean/bioc-traffic-stats` | 7.3 GB | [Package download stats](#package-download-stats-11) |
| Dashboard rollups | `/data/davsean/bioc-traffic-rollups`, copied to `r2:bioc-access-logs/rollups/` | 10 MB | [Dashboard rollups](#dashboard-rollups-10-19) |

CloudFront: **7,501,032,064 rows**, 33 W3C fields, every row, no filtering; every month
reconciles against source (`--verify`). The Iceberg table is *metadata over the same Parquet
objects*, not a second copy, and nothing in the DuckDB path reads it.

## Cloudflare era — logging since the Worker (ADR 0003, amended)

Per [bioc-on-ice#31](https://github.com/seandavi/bioc-on-ice/issues/31), Logpush delivers to
**GCS, not R2** — logging output isolated from operational data, in GCP project `bioc-u24`.
Delivery uses Logpush's S3-compatible endpoint with HMAC keys
(`bioc-u24-logpush-hmac-access-id` / `bioc-u24-logpush-hmac-secret` in `cdsci-infra`).

| Artifact | Location | Notes |
|---|---|---|
| bioc-site Worker records | `gs://bioc-u24-logs/cloudflare/bioc-access-logs/{DATE}/` | Logpush job **1826554**; the system of record |
| bioc-on-ice Worker records | `gs://bioc-u24-logs/cloudflare/bioc-on-ice/{DATE}/` | Logpush job **1827410** |
| Account-wide, unfiltered | `gs://bioc-u24-logs/cloudflare/cloudflare-managed-d9dc0363/{DATE}/` | job **1360530**; bioc records land here too (filter: seandavi/omicidx#142) |
| CloudFront Parquet (GCS copy) | `gs://bioc-u24-logs/cloudfront/parquet/` | same 80 files as the R2/local mirrors |

**The record uses CloudFront's field names verbatim** (`worker/src/keys.ts` `accessRecord`),
all 33 columns except `date`/`time` (derivable from `ts`), with `null` — never zero — for what
the edge cannot know (`cs_bytes`, `time_to_first_byte`, `c_port`, `fle_*`) or deliberately does
not collect (`cs_cookie`). Plus Cloudflare-only extras under `cf_*` (ASN, org, country, RTT).
It rides inside the Logpush trace envelope as a JSON string in `Logs[].Message[]` — invisible
in `cf_workers_trace_raw`'s autodetected schema, which shows only the envelope.

**The normalizing view exists: `bioc-u24.logs.cloudflare_access`** — `UNNEST` + `JSON_VALUE`
over the raw table, filtered to `type = 'access'`, presenting the identical 35 columns, names,
order and types as `cloudfront_raw` (verified), so cross-era queries are a plain `UNION ALL`.
Unit conversions live there too: `time_taken` ms→seconds, `cs_uri_query` sheds its leading `?`.
Definition: `bq show --format=prettyjson bioc-u24:logs.cloudflare_access`.

**BigQuery is not used any more.** Every query over `cf_workers_trace_raw` has failed since
2026-08-14 (scheduled-event records, #1); DuckDB over the Parquet below replaced it (#7). What
was there, for the record: dataset `bioc-u24:logs`, external tables over the objects
in place — `cf_workers_trace_raw` (NDJSON, bioc-access-logs), `icegate_trace_raw` (NDJSON,
bioc-on-ice), `cloudfront_raw` (Parquet, the historical era). Both eras side by side, e.g.:

```bash
bq query --project_id=bioc-u24 --use_legacy_sql=false \
  'SELECT DATE(TIMESTAMP_MILLIS(EventTimestampMs)) d, COUNT(*) n
   FROM `bioc-u24.logs.cf_workers_trace_raw` GROUP BY d ORDER BY d DESC LIMIT 7'
```

**Hourly Parquet (#8): `cloudflare-logs-to-parquet.py`.** It parses the envelope once and
writes one file per UTC hour of `ts` to `/data/davsean/bioc-cloudflare-parquet`
(`year=/month=/day=/hour=/logs.parquet`, zstd, sorted by `ts`), then uploads to
`r2:bioc-access-logs/parquet/cloudflare/` if verify passes. The columns are the 33 CloudFront
columns (same names, order and types, with the BigQuery view's conversions), then `ts`, `cf_*`,
`cf` (JSON text) and `client_id`. The run log counts skipped messages (non-JSON `waitUntil()`
warnings, non-access types). `--verify` compares access records per hour in the raw objects
with the Parquet. Measured on 2026-09-29: 6,367,023 records, **522 MB/day** (`cf` is 40% of
that, `client_id` 13%), 45 s from GCS. Query both eras through `sql/access.sql`
(`just duckdb`); `client_id` agrees across eras (all 104,856 IPs seen in both map to the same id).

- **Records arrive late.** The trace event is emitted when the invocation finishes, so a
  long download's record can land in an object up to hours after its `ts` (measured max 3.0 h).
  Each hour is therefore built from objects up to 6 h past it, and the 08:00 UTC re-seal of
  yesterday is the final version. The 15-minute run's last hours can be short until then.
  A record later than 6 h is not written, and the run log counts it (`late beyond 6:00:00`).
- **Coverage and the cutover.** Logpush to GCS starts 2026-08-06T20:19Z. Until the
  2026-09-28 ~20:09Z cutover the Worker served only `bioc-dev.cancerdatasci.org`; production
  stayed on CloudFront, which still serves a cached-DNS trickle after it. A request is in
  exactly one era, so `access` has no time cut: filter `production` instead (#15, #16).

Gotchas and state:

- **R2 bucket `bioc-access-logs`: the date prefixes are the abandoned pre-switch Logpush
  destination** (only 2026-08-06 → 08-07T12:17Z, never growing; not data loss). `parquet/`
  and `rollups/` in the same bucket are live (this repo writes them).
- **Analytics Engine (`bioc_site_requests_v3`) is a dashboard, never the record** — sampled,
  three-month retention (ADR 0003).
- **Gap monitoring: `bioc-logpush-check.timer`** (daily 07:15 MDT) runs `check-logpush.sh`,
  which fails (filing a GitHub issue via `bioc-notify@`) unless every UTC hour of yesterday has
  an object and the day has ≥ `MIN_OBJECTS` (1000; ~2,600/day since the cutover). Logpush
  cannot backfill, so this is the alarm ADR 0003 requires. `./check-logpush.sh 20260928`
  checks any day. **Dead-man:** `bioc-logpush-check-stale.timer` (12:00) fails if the check
  has not passed in 36 h (`~/.local/state/bioc-logpush-check.ok`).

## Package download stats (#11)

`download-stats.py` (`just stats`; timer `bioc-download-stats`, 03:30 MDT) writes under
`/data/davsean/bioc-traffic-stats`, nothing public:

- `clients/year=/month=/clients.parquet` — per (date, category, package, bioc_version,
  client_id, client_class, era) download counts from the `downloads` view (`sql/downloads.sql`,
  the DOWNLOADS_SQL filter), production hosts only. Internal (carries `client_id`). A month is
  rebuilt only when a source Parquet file of that month is newer, so CloudFront history is
  computed once. 81 months (2020-01 → 2026-09), 7.1 GB; the backfill took ~1 h 20 min
  (10–240 s a month on a loaded host, 2026-09-30).
- `package_month`, `package_day` (last 90 days), `release_month`, `category_month`,
  `overall_month` `.parquet` — fixed columns `downloads`, `distinct_clients`; interpretive
  `downloads_{human,package_client,automated}` + `distinct_clients_*` (client_class_v0),
  `rule_version`, `era`. Rebuilt every run (~15 min).
- `stats/` — the `/packages/stats/` tree of ADR 0004 from the fixed columns (~13 min).

Things that shape the numbers:

- **CloudFront 302s any `/packages/<v>/<cat>/…_x.tar.gz`**, so the raw filter yields junk
  names (July 2026 workflows: ~1,800 "packages"). Aggregates and the tree count only
  (category, package) pairs listed in some release's `src/contrib/PACKAGES`
  (`packages-index/`, cached; release and devel refetched each run).
- **Category comes from the URL**, as in DOWNLOADS_SQL. The published files sometimes also count
  a package's requests under *other* category paths (DESeq2 2026-07: published 116,410 =
  bioc 91,752 + data-annotation 12,141 + data-experiment 6,145 + workflows 6,105 within
  0.2%), but not consistently (applying that rule to all months puts BiocGenerics 2024-12 at
  +66%). Ordinary months run 0.2–0.9% below published for bioc packages.
- **The human/automated split steps at the 2026-09-28 cutover** for classifier reasons: only
  the Cloudflare era has `bot_category` and `cf_asn`. The fixed columns are continuous
  across it.
- **Mid-Aug → Sep 2026 is ~2× normal** in both published and ours: `likely_automated`
  (~15k clients, ~4M downloads a week) plus ~1.9M distinct browser-UA clients a week.
- **bioconductor.org returns 403 to the Python-urllib User-Agent** (since it moved behind
  Cloudflare); send one.

## Dashboard rollups (#10, #19)

`just rollup minute|hour|day [--full|--since-yesterday]` writes static files (ADR 0004) to
`/data/davsean/bioc-traffic-rollups` and copies them to `r2:bioc-access-logs/rollups/`. Not
public. No `c_ip`; `clients` is a distinct `client_id` count.

| File | Grain, window | Refreshed |
|---|---|---|
| `minute.{parquet,json}` | minute, last 6 h | every 15 min, full recompute |
| `hour.{parquet,json}` | hour, last 30 d | hourly: last few hours only; daily: yesterday + today |
| `day.{parquet,json}` | day, last 90 d | daily, after the 08:00 UTC re-seal |
| `overall_day.{parquet,json}` | day, both eras since 2020-01-01, `dimension = 'overall'` only | daily, last 3 days |
| `window_clients.{parquet,json}` | exact distinct clients over the 30 d / 90 d windows, overall and per class | daily |

Tier rows: `t, dimension, value, client_class, rule_version, requests, bytes, clients`.
`dimension` is one of `overall`, `status`,
`status_class`, `country` (Cloudflare era only), `ua_family`, `page`, `referrer` (host),
`cache`, `package`, `bioc_version`. **`client_class` NULL is the all-classes total**; filter
it, or sum the classes, never both.

- **Top-N is per bucket** in hour and day: `page`, `referrer`, `ua_family` and `package` keep
  each bucket's top 25 and fold the rest into `(other)`. Summing them over a window is
  approximate. Minute keeps a window top-N.
- **`clients` does not add across buckets.** Window rows in `hour.json`/`day.json` sum
  requests and bytes and leave `clients` null; use `window_clients` for those.
- **Incremental = full** is checked by `sql/rollup_merge_check.sql`. Rebuild a tier with
  `--full` after a `client_class` version bump (hour ~13 min, day ~1 h).

## Querying

Load the views with `just duckdb` (sets the `client_id` salt from GSM, loads
`sql/{access,client_class,downloads}.sql`). Views: `access` (both eras, with `era`,
`client_id`, `production`, `cf_asn`, `bot_category`, `cf_country`), `cloudfront_access`,
`cloudflare_access` (all Cloudflare columns, incl. `cf` JSON), `downloads` (the published
download definition, plus `package`, `category`, `bioc_version`, `client_class`). Filter on
`year`/`month`, the Hive partitions, or every query scans 7.5 B rows.

```bash
# Requests and clients on the cutover day, by era (~20 s)
just duckdb -c "SELECT era, count(*) n, count(DISTINCT client_id) clients FROM access
                WHERE year=2026 AND month=9 AND day(date)=28 AND production GROUP BY 1"

# Package downloads by month, fixed + human columns (static file, instant)
duckdb -c "SELECT month, downloads, distinct_clients, downloads_human, era
           FROM '/data/davsean/bioc-traffic-stats/package_month.parquet'
           WHERE package='limma' AND category='bioc' ORDER BY month DESC LIMIT 12"

# Last 24 h by traffic class (rollups; NULL class = total)
duckdb -c "SELECT client_class, sum(requests) FROM '/data/davsean/bioc-traffic-rollups/hour.parquet'
           WHERE dimension='overall' AND t >= now() AT TIME ZONE 'UTC' - INTERVAL 24 HOUR
           GROUP BY 1 ORDER BY 2 DESC"

# Top countries, last 7 days
duckdb -c "SELECT value, sum(requests) FROM '/data/davsean/bioc-traffic-rollups/day.parquet'
           WHERE dimension='country' AND client_class IS NULL AND t >= current_date - 7
           GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
```

Prefer the rollups and stats files: they answer most questions in milliseconds. Go to `access`
only for something they don't carry.

## Timers on onclappc02

All are user units symlinked from `systemd/` (install commands in README); failures file a
GitHub issue via `bioc-notify@`. Times MDT.

| Timer | When | Does |
|---|---|---|
| `bioc-logpush-check` | 07:15 | yesterday's delivery: every hour + volume |
| `bioc-logpush-check-stale` | 12:00 | dead-man on the check above |
| `bioc-cloudflare-parquet` | every 15 min | current + previous UTC hour → Parquet → R2 |
| `bioc-cloudflare-parquet-reseal` | 02:00 | re-seal yesterday (late records) |
| `bioc-download-stats` | 03:30 | stats tree and aggregates |
| `bioc-rollup-minute` | every 15 min | minute tier |
| `bioc-rollup-hour` | hourly at :20 | hour tier, recent hours |
| `bioc-rollup-day` | 04:30 | hour tier since yesterday, day tier, overall series, window clients |

Logs: `journalctl --user -u <unit>`; also shipped to ClickHouse (`default.systemd_user_jobs`).

## Connecting

### Trino — **not currently running**

Torn down 2026-08-05; PyIceberg does everything it was needed for. The config survives at
`/data/davsean/bioc-trino` and **contains no secrets** (three `${ENV:...}` refs resolved at run
time). Recreate only if you want ad-hoc SQL over the catalog:

```bash
docker exec -it bioc-trino trino --catalog biocr2 --schema cloudfront   # once running
```

From the host: `http://localhost:18080`, JDBC `jdbc:trino://localhost:18080/biocr2/cloudfront`,
any username, no auth, no TLS.

```bash
docker run -d --name bioc-trino -p 18080:8080 \
  -e CF_TOKEN="$(gcloud secrets versions access latest --secret=cdsci-cloudflare-api-token --project=cdsci-infra)" \
  -e R2_KEY="$(gcloud secrets versions access latest --secret=cdsci-r2-access-key-id --project=cdsci-infra)" \
  -e R2_SECRET="$(gcloud secrets versions access latest --secret=cdsci-r2-secret-access-key --project=cdsci-infra)" \
  -v /data/davsean/bioc-trino/config.properties:/etc/trino/config.properties:ro \
  -v /data/davsean/bioc-trino/jvm.config:/etc/trino/jvm.config:ro \
  -v /data/davsean/bioc-trino/catalog:/etc/trino/catalog:ro \
  trinodb/trino:latest
```

Boot takes 60–90 s. Queries fail with *"server is still initializing"* until
`curl -s localhost:18080/v1/info` reports `"starting": false`.

### DuckDB — local Parquet (fastest for iteration)

```sql
SELECT * FROM read_parquet('/data/davsean/bioc-cf-parquet/**/*.parquet',
                           hive_partitioning=true) LIMIT 5;
```

### DuckDB — Parquet in R2

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET r2 (TYPE s3, KEY_ID '…', SECRET '…',
  ENDPOINT '<account>.r2.cloudflarestorage.com', REGION 'auto', URL_STYLE 'path');
SELECT * FROM read_parquet('s3://bioc-cloudfront-logs/parquet/**/*.parquet',
                           hive_partitioning=true) LIMIT 5;
```

Account ID and keys: `cdsci-r2-account-id`, `cdsci-r2-access-key-id`,
`cdsci-r2-secret-access-key` in gcloud project `cdsci-infra`.

### DuckDB — the Iceberg table

```sql
INSTALL iceberg; LOAD iceberg;
CREATE SECRET ice (TYPE ICEBERG, TOKEN '<cdsci-cloudflare-api-token>');
ATTACH '<account>_bioc-cloudfront-logs' AS ice
  (TYPE ICEBERG, ENDPOINT 'https://catalog.cloudflarestorage.com/<account>/bioc-cloudfront-logs');
```

## Engine capabilities, measured not assumed

| | Iceberg read | Create partitioned | Adopt existing Parquet | Upsert / MERGE | Available |
|---|:--:|:--:|:--:|:--:|---|
| **PyIceberg** | yes | yes | **yes, partitioned too** | **yes** | `pip install 'pyiceberg[s3fs]'` |
| Trino | yes | yes | yes, **unpartitioned only** | yes | `bioc-trino` container |
| DuckDB | yes | **no** | no | **no** (INSERT/UPDATE/DELETE only) | local |
| StarRocks | yes | yes | unclear | yes | `starrocks-fe` / `-be` running |
| ClickHouse | yes | **no** | no | no | container + local 26.7.2.59 |

**PyIceberg is the one to reach for.** It is the only tool here that adopts existing Parquet into
a *partitioned* table, it does real upsert, and it runs in-process — no container, no JVM, no
90-second boot. Trino's only unique claim was `add_files`, and PyIceberg does it better.

Use DuckDB for everything you query, PyIceberg for Iceberg maintenance, StarRocks if you want
materialized views. Trino is now a convenience, not load-bearing.

## Writing

### Adopt existing Parquet — zero copy, the normal path for log batches

```python
from pyiceberg.catalog.rest import RestCatalog
cat = RestCatalog("r2",
    uri=f"https://catalog.cloudflarestorage.com/{acct}/bioc-cloudfront-logs",
    warehouse=f"{acct}_bioc-cloudfront-logs", token=CF_TOKEN,
    **{"s3.endpoint": f"https://{acct}.r2.cloudflarestorage.com",
       "s3.access-key-id": R2_KEY, "s3.secret-access-key": R2_SECRET, "s3.region": "auto"})
t = cat.load_table("cloudfront.access_logs")
t.add_files(["s3://bioc-cloudfront-logs/parquet/year=2026/month=8/logs.parquet"])
```

~3 s per file, metadata only. **`check_duplicate_files=True` is the default and is the
incremental primitive** — re-adding a known file raises rather than duplicating it. Combined
with immutable, uniquely-named source objects, an incremental run is "list everything, add
everything, let Iceberg reject what it has". Idempotent by construction, and it transfers
unchanged to Logpush objects later.

Trino equivalent, for unpartitioned tables only:

```sql
ALTER TABLE biocr2.cloudfront.access_logs EXECUTE add_files(
  location => 's3://bioc-cloudfront-logs/parquet',
  format => 'PARQUET', recursive_directory => 'true');
```

### Upsert — for derived tables, not for logs

```python
import duckdb
batch = duckdb.connect().execute(sql).fetch_arrow_table().cast(table_schema)
res = t.upsert(batch, join_cols=["k"])     # res.rows_updated / res.rows_inserted
```

`upsert()` takes a `pa.Table`, so **the producer is anything that speaks Arrow** — DuckDB,
Polars, pandas. Polars is not required and is not installed; DuckDB is already here and faster.

### Which to use

- **Raw log batches → `add_files`.** Append-only, immutable, no key to update. Zero copy.
- **Derived tables → `upsert`.** Sessionized journeys, the engagement graph, rolling aggregates.
- **Published statistics → neither.** A full regeneration is 27 s (see Performance). Recompute
  beats merge at that price and avoids the whole class of incremental-correctness bugs — which
  is exactly the failure that cost the live system a quarter of June 2026.

## Gotchas, each of which cost real time

- **Never set `comment='#'` when reading CloudFront logs with DuckDB.** It treats `#` as a
  comment marker *mid-line*, so any record whose user-agent or URI contains one truncates
  there. A Sogou crawler UA ending `webmasters.htm#07` truncates to 11 fields; combined with
  `ignore_errors=true` one 9,168-line file yielded 93 rows. Use `null_padding=true` and filter
  `column00 NOT LIKE '#%'` instead.
- **DuckDB reports 35 columns for these Parquet files; they contain 33.** `year` and `month` are
  synthesised from the Hive path. Iceberg DDL must use the true 33, and partitioning must use a
  transform (`month(date)`), not stored columns.
- **`add_files` does not validate schemas** and **refuses partitioned tables.** Check the schema
  yourself; a mismatch produces wrong data, not an error.
- **Trino 482 renamed `s3.aws-secret` to `s3.aws-secret-key`.** The older spelling fails at
  `loadInitialCatalogs` and the container exits — check `docker logs`, not just `docker ps`,
  which reports `Up (unhealthy)`.
- **Trino won't run under `--user`** (its `/data/trino` isn't writable), and its default UID
  can't read `0600` files. Pass secrets as env vars rather than loosening permissions.
- **R2 Data Catalog does not vend credentials.** The REST catalog authenticates with an OAuth2
  bearer token *and* FileIO needs separate static R2 S3 keys.
- **ETag comparison is meaningless for multipart objects.** 428 objects in the raw archive were
  uploaded multipart, so `rclone check` cannot hash them; `gzip -t` verifies them properly.
- **Do not verify with parallel `zcat` into one pipe.** Interleaved writes split records at
  buffer boundaries and inflate counts — this manufactured a phantom 28,746-row loss. The
  giveaway was anomalous field counts pairing to sum 66.
- **DuckDB cannot MERGE into Iceberg.** Native `MERGE INTO` works fine; the Iceberg extension
  rejects it (`Database type "iceberg" does not support MERGE INTO or ON CONFLICT`). `INSERT`,
  `UPDATE` and `DELETE` all work, so delete-then-insert in a transaction is the fallback — but
  PyIceberg's `upsert()` is the real answer.
- **PyIceberg `add_files` fails on an *empty* table** — `ArrowInvalid: Must pass at least one
  table`, because the duplicate check calls `pa.concat_tables()` on an empty list. Pass
  `check_duplicate_files=False` for the first add only.
- **DuckDB's `.arrow()` returns a `RecordBatchReader`, which PyIceberg rejects.** Use
  `.fetch_arrow_table()`.
- **Don't match on process command lines to wait for a job.** `pgrep -f "foo"` matches the
  shell running it, so the wait never ends; `pkill -f 'while pgrep -f'` killed the job it was
  meant to protect. This cost three deadlocks in one session. Capture PIDs explicitly.
- **Trino reports `Up (unhealthy)` while dying.** A catalog config error kills it during
  `loadInitialCatalogs` minutes after start — check `docker logs`, not `docker ps`, and don't
  read "no errors yet" during the boot window as success.

## Sharing access — read this before minting any token

**R2 Data Catalog cannot be scoped to a bucket.** It requires `Admin Read only` or
`Admin Read & Write`, and both are **account-wide**. The bucket-scopable permissions
(`Object Read only` / `Object Read & Write`) work only with the S3-compatible API, not the
catalog's REST API. Cloudflare also documents that catalog-vended credentials **inherit the
token's storage permissions**.

Consequence: **a collaborator token that can reach the catalog can also read
`bioc-cloudfront-logs`, raw client IPs included.** Putting public data in a separate *bucket*
does not isolate it at the catalog layer.

Two ways to share safely:

- **Bucket-scoped S3 keys, no catalog.** `Object Read only` scoped to one bucket yields an
  Access Key ID and Secret reaching only that bucket. Consumers query Parquet directly
  (`read_parquet('s3://…/**/*.parquet', hive_partitioning=true)`), and still get Iceberg
  semantics via `StaticTable.from_metadata('s3://…/metadata/….metadata.json')` — no catalog
  needed, snapshots and schema evolution intact, discovery via a published manifest.
- **A separate Cloudflare account for public data.** Then `Admin Read only` is harmless because
  the account holds nothing else. This is the right shape for a public lake, and it argues for
  provisioning it as its own account rather than retrofitting later.

Note also that R2 API tokens *derive* S3 credentials: **Access Key ID = the token's `id`,
Secret Access Key = SHA-256 of the token `value`**. One token yields both.

Neither `cdsci-cloudflare-api-token` nor `cdsci-cloudflare-workers-token` can create tokens —
that needs the dashboard or a key with API-token-write permission.

## De-identification

**The record carries the raw client address; `client_id` is computed in the view (ADR 0012,
superseding 0009).** Every delivered record is `v: 1` with `c_ip` and `x_forwarded_for`
populated — the edge-hashing change was never deployed and has been reverted. The normalizing
view (or a derived table, as an *additional* column) computes `client_id = sha256(salt || c_ip)`
with the same salt and encoding for both eras. Salt is `bioc-logs-ip-salt`; it is **not**
deployed to the Worker. Anything that crosses the publication boundary carries `client_id`
and never `c_ip`.

The projection below is the `client_id` definition for **both eras**; historically it ran once
over the CloudFront era (#94). Intended for the current stats maintainers. Design and salt from 2026-08-05:

- `c_ip` → `client_id = sha256(secret_salt || c_ip)`. Salt is 32 random bytes, **stored and used
  as their 64-character hex string**, in **`bioc-logs-ip-salt`** (gcloud `cdsci-infra`), created
  2026-08-05 — same value and same encoding the Worker uses, or the eras split. Keyed because an unsalted
  SHA-256 of an IPv4 address is trivially reversible — only 4 billion candidates. **Pseudonymous,
  not anonymous**: the same client maps to the same id, which is what makes distinct-client
  counts meaningful and is also the residual risk. Salt is deliberately stable; rotating it
  would break longitudinal counts.
- Dropped: `x_forwarded_for` (0.5% populated, carries IP chains), `cs_cookie` (2.6%, highest
  risk), `cs_uri_query` (0.9%, may carry tokens). All sparse — negligible analytic loss.
- Kept: `cs_referer` with query string stripped (14% populated, enables journey analysis).
- **No status or method filter.** Both existing pipelines bake in "redirects count, 206 does
  not, HEAD excluded", and those conventions are exactly what is disputed. Hand over the raw
  population and let consumers apply their own policy.
- Measured while building: ~6–30 s per month, ~6.4 GB for 2020-01 through 2025-12, so the full
  set is roughly 8 GB — small enough for a collaborator to pull entirely.

## Rebuilding

```bash
./cloudfront-logs-to-parquet.py --self-check                      # verify the downloads view
./cloudfront-logs-to-parquet.py --from 2020-01 --to 2026-08 \
    --logs-dir /data/davsean/bioc-cf-logs-raw --out /data/davsean/bioc-cf-parquet
./cloudfront-logs-to-parquet.py --verify --from 2020-01 --to 2026-08 \
    --logs-dir /data/davsean/bioc-cf-logs-raw --out /data/davsean/bioc-cf-parquet
```

Roughly 2.5 h from local disk. Re-adopting into Iceberg after a rebuild:

```sql
DROP TABLE biocr2.cloudfront.access_logs;   -- CREATE OR REPLACE is unsupported
-- recreate with the 33-column DDL, then:
ALTER TABLE biocr2.cloudfront.access_logs EXECUTE add_files(
  location => 's3://bioc-cloudfront-logs/parquet',
  format => 'PARQUET', recursive_directory => 'true');
```

80 files adopted in **47 seconds** — metadata only. A DuckDB rewrite of the same data was
measured at **15 hours**, which is why Trino exists in this stack.

## Performance

| Query | Trino → Iceberg/R2 | DuckDB → R2 | **DuckDB local** |
|---|---:|---:|---:|
| Single-month count | 13.2 s | 1.1 s | **0.1 s** |
| Top packages, one month (2 regexes, 175M rows) | 14.9 s | 11.1 s | **1.0 s** |
| Monthly totals, full scan of 7.1B rows | 291.7 s | 277.0 s | **27.2 s** |

Two conclusions.

**Compute locally.** On the full scan the engines are effectively tied over the network
(277 s vs 292 s) — the link is the bottleneck, not the engine. Off local disk the same query is
27 s, a 10× difference that has nothing to do with which engine you picked. R2 is the durable
copy and the multi-engine access point; it is not the hot path.

**A full regeneration of the published series takes 27 seconds.** That is what makes ADR 0004's
"recompute rather than patch forward" close to free, and why no incremental machinery is needed
for the statistics themselves.

Cross-engine agreement is exact — Trino over Iceberg and DuckDB over Parquet return identical
counts (175,314,722 for July 2026; `BiocGenerics` 294,409; 2020-01 at 2,189,884). Two engines,
two access paths, same numbers. That also validates `add_files`: Trino was reading through
Iceberg metadata for files it never wrote, and landed on the same answers.

**Benchmarking traps, both of which produced badly misleading numbers here.** Filter on
`year`/`month`, not `date` — the Hive partitions are the former, and a `date`-only filter forces
all 80 files open (45 s instead of 0.1 s). And leave `threads` at the default; capping DuckDB at
16 of 64 cores made Trino look 3× faster than it is.
