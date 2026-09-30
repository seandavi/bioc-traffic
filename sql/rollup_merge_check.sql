-- rollup_merge = a full per_bucket rebuild over a 3-day retention, hour and day tiers: a tier
-- stored by an earlier run (5 h, 1 day), trimmed and brought up to t1 from rollup_start, vs
-- rollup_tier over the whole window. Fails (non-zero exit) on any differing row. t1 is 2 h
-- back, so the 15-min Parquet job does not rewrite an hour mid-check.
--   just duckdb -c ".read sql/rollup_tier.sql" -c ".read sql/rollup_merge_check.sql"
SET VARIABLE t1 = date_trunc('hour', now() AT TIME ZONE 'UTC') - INTERVAL 2 HOUR;
SET VARIABLE retain = INTERVAL 3 DAY;

SET VARIABLE h_prev = getvariable('t1') - INTERVAL 5 HOUR;
SET VARIABLE h0 = date_trunc('hour', getvariable('t1') - getvariable('retain'));
COPY (FROM rollup_tier('hour', date_trunc('hour', getvariable('h_prev') - getvariable('retain')),
                       getvariable('h_prev'), per_bucket := true))
  TO '/data/davsean/tmp/rollup_merge_check_hour.parquet';
SET VARIABLE hi = rollup_start('hour', getvariable('h0'), getvariable('t1'),
    (SELECT max(t) FROM '/data/davsean/tmp/rollup_merge_check_hour.parquet'));

SET VARIABLE d_prev = getvariable('t1') - INTERVAL 1 DAY;
SET VARIABLE d0 = date_trunc('day', getvariable('t1') - getvariable('retain'));
COPY (FROM rollup_tier('day', date_trunc('day', getvariable('d_prev') - getvariable('retain')),
                       getvariable('d_prev'), per_bucket := true))
  TO '/data/davsean/tmp/rollup_merge_check_day.parquet';
SET VARIABLE di = rollup_start('day', getvariable('d0'), getvariable('t1'),
    (SELECT max(t) FROM '/data/davsean/tmp/rollup_merge_check_day.parquet'));

CREATE TEMP TABLE m AS
    SELECT 'hour' AS grain, * FROM rollup_merge('hour', '/data/davsean/tmp/rollup_merge_check_hour.parquet',
                                                getvariable('h0'), getvariable('hi'), getvariable('t1'))
    UNION ALL
    SELECT 'day', * FROM rollup_merge('day', '/data/davsean/tmp/rollup_merge_check_day.parquet',
                                      getvariable('d0'), getvariable('di'), getvariable('t1'));
CREATE TEMP TABLE f AS
    SELECT 'hour' AS grain, * FROM rollup_tier('hour', getvariable('h0'), getvariable('t1'), per_bucket := true)
    UNION ALL
    SELECT 'day', * FROM rollup_tier('day', getvariable('d0'), getvariable('t1'), per_bucket := true);
WITH diff AS (
    SELECT grain, count(*) AS n FROM ((FROM m EXCEPT ALL FROM f) UNION ALL (FROM f EXCEPT ALL FROM m))
    GROUP BY grain
)
SELECT CASE WHEN (SELECT count(*) FROM diff) = 0
            THEN 'rollup_merge = full: hour ' || (SELECT count(*) FROM m WHERE grain = 'hour')
                 || ' rows (from ' || getvariable('hi') || '), day '
                 || (SELECT count(*) FROM m WHERE grain = 'day') || ' rows (from ' || getvariable('di') || ')'
            ELSE error('rollup_merge differs from full: '
                       || (SELECT string_agg(grain || ' ' || n || ' rows', ', ') FROM diff)) END AS result;
