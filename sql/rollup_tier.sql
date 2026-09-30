-- Dashboard rollups, one tier at a time (seandavi/bioc-traffic#10). DuckDB.
--
-- Load after sql/access.sql and sql/client_class.sql (`just duckdb` loads both):
--   FROM rollup_tier('hour', TIMESTAMP '2026-09-29', TIMESTAMP '2026-09-30')
--
-- One long table per tier: t, dimension, value, client_class, rule_version, requests, bytes,
-- clients. Grouping sets, so NULL means "all": client_class NULL is every class, t NULL is the
-- whole window. clients (distinct client_id) is exact per row and does not add across rows.
--
-- Dimensions: overall ('all'), status_class ('2xx'), status, country (Cloudflare era only;
-- CloudFront rows have none, not 'unknown'), ua_family, page, referrer (host), cache
-- (x_edge_result_type), package and bioc_version (package downloads only). page, referrer,
-- ua_family and package keep their top_n values over the window, by requests; the rest
-- is '(other)'. per_bucket := true (the hour and day tiers) keeps the top_n per t instead and
-- drops the t NULL rows, so each t depends only on its own requests and a stored tier can be
-- updated a few buckets at a time (rollup_merge, #19).
--
-- Production traffic only. Classes step at the 2026-09-28 cutover for classifier reasons:
-- the CloudFront era has no ASN or bot label (#11 comment), so publish with rule_version.

-- Coarse client software, for the dashboard: R, curl, python, the browser, or the bot's own
-- name. u is client_ua_v0(ua). Not a class: a browser UA can be a crawler (client_class).
CREATE OR REPLACE MACRO ua_family_v0_(u) AS CASE
    WHEN u IS NULL OR u IN ('', '-') THEN '(none)'
    WHEN regexp_matches(u, '(^|[ ;])r \([0-9]|(^| )r/[0-9]|rstudio|^renv |biocmanager|r-curl/|'
                        || 'httr2?/') THEN 'R'
    WHEN regexp_matches(u, '^libcurl/') THEN 'libcurl'
    WHEN regexp_matches(u, '^curl/') THEN 'curl'
    WHEN regexp_matches(u, '^wget/') THEN 'wget'
    WHEN regexp_matches(u, 'python|aiohttp') THEN 'python'
    WHEN regexp_matches(u, '[a-z0-9_-]*(bot|spider|crawler)')
        THEN regexp_extract(u, '[a-z0-9_-]*(bot|spider|crawler)')
    WHEN regexp_matches(u, 'go-http-client') THEN 'go'
    WHEN regexp_matches(u, '^java/|okhttp|apache-httpclient') THEN 'java'
    WHEN regexp_matches(u, 'headlesschrome') THEN 'headless chrome'
    WHEN regexp_matches(u, 'edg/') THEN 'Edge'
    WHEN regexp_matches(u, 'opr/|^opera') THEN 'Opera'
    WHEN regexp_matches(u, 'firefox/|fxios/') THEN 'Firefox'
    WHEN regexp_matches(u, 'chrome/|crios/') THEN 'Chrome'
    WHEN regexp_matches(u, 'safari/') THEN 'Safari'
    WHEN regexp_matches(u, 'msie |trident/') THEN 'IE'
    ELSE 'other'
END;

CREATE OR REPLACE MACRO ua_family_v0(ua) AS ua_family_v0_(client_ua_v0(ua));

-- The fixed package-download definition (DOWNLOADS_SQL in cloudfront-logs-to-parquet.py, the
-- downloads view in sql/downloads.sql): tarball and binary URIs, the status codes it counts,
-- no HEAD. Keep all three in step.
CREATE OR REPLACE MACRO package_download_v0(uri, status, method) AS
    regexp_matches(uri, '^/+packages/+[^/]+/+(bioc|workflows|data/+experiment|data/+annotation)'
                        || '/+(bin|src)/+.*_.*\.(tar\.gz|zip|tgz)$')
    AND status IN ('200', '301', '302', '307', '308') AND method <> 'HEAD';

-- Production requests with every rollup dimension derived.
CREATE OR REPLACE VIEW hits AS
SELECT ts, year, date,
       -- ponytail: a 64-bit hash of client_id for count(DISTINCT); collisions ~n²/2^65, nil
       -- at millions of clients.
       hash(client_id) AS client_key,
       TRY_CAST(sc_bytes AS BIGINT) AS bytes,
       client_class_v0(cs_user_agent, cs_uri_stem, cs_method,
                       bot_category := bot_category, asn := cf_asn) AS client_class,
       'all' AS overall,
       left(sc_status, 1) || 'xx' AS status_class,
       sc_status AS status,
       cf_country AS country,
       ua_family_v0(cs_user_agent) AS ua_family,
       cs_uri_stem AS page,
       coalesce(nullif(lower(regexp_extract(cs_referer, '^[a-zA-Z][a-zA-Z0-9+.-]*://([^/:?#]+)', 1)),
                       ''), '(none)') AS referrer,
       -- CloudFront says Hit/Miss/RefreshHit/Error, Cloudflare HIT/MISS/PASS/RANGE.
       upper(x_edge_result_type) AS cache,
       CASE WHEN package_download_v0(cs_uri_stem, sc_status, cs_method)
            THEN regexp_extract(cs_uri_stem, '/([^/_]+)_[^/]*\.(tar\.gz|zip|tgz)$', 1) END AS package,
       CASE WHEN package_download_v0(cs_uri_stem, sc_status, cs_method)
            THEN regexp_extract(cs_uri_stem, '^/+packages/+([^/]+)/', 1) END AS bioc_version
FROM access
WHERE production;

-- Tier windows: minute grain for 6 h, hour for 30 d, day for 90 d.
CREATE OR REPLACE MACRO rollup_window(grain) AS CASE grain
    WHEN 'minute' THEN INTERVAL 6 HOUR
    WHEN 'hour' THEN INTERVAL 30 DAY
    WHEN 'day' THEN INTERVAL 90 DAY
    ELSE error('grain must be minute, hour or day') END;

-- How far back an incremental run recomputes: whatever upstream can have rewritten since the
-- last run. The 15-min Parquet timer rewrites the current and previous UTC hour, so hourly runs
-- redo 3 hours; the 08:00 UTC re-seal rewrites yesterday, so the daily run (after it) redoes
-- yesterday and today, for the day tier and, with --since-yesterday, the hour tier. Change
-- these with the Parquet timers.
CREATE OR REPLACE MACRO rollup_late(grain) AS CASE grain
    WHEN 'hour' THEN INTERVAL 2 HOUR
    WHEN 'day' THEN INTERVAL 1 DAY
    ELSE error('grain must be hour or day') END;

-- Where an incremental run starts: rollup_late back from t1, or since if earlier, or the last
-- stored t if runs were missed; never before t0. NULLs are ignored.
CREATE OR REPLACE MACRO rollup_start(grain, t0, t1, last_t, since := NULL) AS
    greatest(t0, least(last_t, date_trunc(grain, t1 - rollup_late(grain)), since));

CREATE OR REPLACE MACRO rollup_tier(grain, t0, t1, top_n := 25, per_bucket := false) AS TABLE
WITH h AS (
    FROM hits
    WHERE ts >= t0 AND ts < t1
      -- Plain comparisons on the Hive year, then date's row-group stats, keep the scan to the
      -- window. An expression over year/month prunes nothing: a 1 h window then took minutes.
      AND year BETWEEN year(t0) AND year(t1) AND date BETWEEN CAST(t0 AS DATE) AND CAST(t1 AS DATE)
),
long AS NOT MATERIALIZED (
    UNPIVOT (SELECT date_trunc(grain, ts) AS t, client_key, bytes, client_class, overall, status_class, status, country,
                    ua_family, page, referrer, cache, package, bioc_version FROM h)
    ON overall, status_class, status, country, ua_family, page, referrer, cache, package, bioc_version
    INTO NAME dimension VALUE value
),
top AS (
    SELECT CASE WHEN per_bucket THEN t END AS top_t, dimension, value AS top_value
    FROM long
    WHERE dimension IN ('page', 'referrer', 'ua_family', 'package')
    GROUP BY top_t, dimension, value
    QUALIFY row_number() OVER (PARTITION BY top_t, dimension ORDER BY count(*) DESC, value) <= top_n
),
folded AS (
    SELECT long.t, long.dimension,
           CASE WHEN long.dimension IN ('page', 'referrer', 'ua_family', 'package') AND top_value IS NULL
                THEN '(other)' ELSE value END AS v,
           client_class, client_key, bytes
    FROM long LEFT JOIN top ON long.dimension = top.dimension AND long.value = top.top_value
                           AND (NOT per_bucket OR long.t = top.top_t)
),
-- One row per client first: a plain GROUP BY spills well, and the DISTINCT below then sees
-- far fewer rows. Straight off folded, a 30 d hour tier took 50 min at 32 GB; this is ~6x
-- faster, same output. (approx_count_distinct was faster still but ~10% off per hour.)
per_client AS (
    SELECT t, dimension, v, client_class, client_key, count(*) AS requests, sum(bytes) AS bytes
    FROM folded
    GROUP BY ALL
)
SELECT t, dimension, v AS value, client_class, client_class_rule_version() AS rule_version,
       CAST(sum(requests) AS BIGINT) AS requests, CAST(sum(bytes) AS BIGINT) AS bytes,
       count(DISTINCT client_key) AS clients
FROM per_client
GROUP BY GROUPING SETS ((t, dimension, v, client_class), (t, dimension, v),
                        (dimension, v, client_class), (dimension, v))
-- ponytail: per_bucket still computes the window sets, then drops them; costs only in --full.
HAVING NOT per_bucket OR t IS NOT NULL
ORDER BY dimension, t NULLS FIRST, requests DESC;

-- A stored per_bucket tier brought up to t1: its rows in [t0, ti) kept, [ti, t1) recomputed,
-- rows before t0 (past retention) dropped. Equals rollup_tier(grain, t0, t1, per_bucket := true)
-- as long as nothing before ti changed upstream since the stored file was written.
CREATE OR REPLACE MACRO rollup_merge(grain, stored, t0, ti, t1) AS TABLE
FROM (FROM read_parquet(stored) WHERE t >= t0 AND t < ti
      UNION ALL
      FROM rollup_tier(grain, ti, t1, per_bucket := true))
ORDER BY dimension, t, requests DESC;

-- Exact distinct clients over the hour (30 d) and day (90 d) tier windows, overall and per
-- client_class (NULL: all classes). The tiers no longer carry window rows, and summed per-t
-- clients double-count; one 90 d scan, so the daily run computes it, not the hourly one.
CREATE OR REPLACE MACRO rollup_window_clients(t1) AS TABLE
WITH w AS (
    SELECT grain, date_trunc(grain, t1 - rollup_window(grain)) AS window_start
    FROM (VALUES ('hour'), ('day')) v(grain)
),
per_client AS (
    SELECT grain, window_start, client_class, client_key, count(*) AS requests, sum(bytes) AS bytes
    FROM hits JOIN w ON ts >= window_start
    WHERE ts < t1
      AND year BETWEEN year(t1 - INTERVAL 91 DAY) AND year(t1)
      AND date BETWEEN CAST(t1 - INTERVAL 91 DAY AS DATE) AND CAST(t1 AS DATE)
    GROUP BY ALL
)
SELECT grain, window_start, client_class, client_class_rule_version() AS rule_version,
       CAST(sum(requests) AS BIGINT) AS requests, CAST(sum(bytes) AS BIGINT) AS bytes,
       count(DISTINCT client_key) AS clients
FROM per_client
GROUP BY GROUPING SETS ((grain, window_start, client_class), (grain, window_start))
ORDER BY grain, client_class NULLS FIRST;
