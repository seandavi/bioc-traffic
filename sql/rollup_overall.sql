-- The forever series (seandavi/bioc-traffic#10): production requests per UTC day and era, both
-- eras, overall only, so it stays cheap. DuckDB, after sql/access.sql:
--   FROM rollup_overall_day(DATE '2026-09-29', DATE '2026-09-30')
--
-- `just rollup-overall` computes the history once, then recomputes the last 3 days and keeps
-- the rest; `just rollup day` runs it. era separates the seam instead of smoothing it; no
-- client_class here, since the classes are not comparable across eras (#11). clients
-- (distinct client_id) is per day and era and does not add across rows.

CREATE OR REPLACE MACRO rollup_overall_day(d0, d1) AS TABLE
SELECT date AS day, era, count(*) AS requests,
       CAST(sum(TRY_CAST(sc_bytes AS BIGINT)) AS BIGINT) AS bytes,
       count(DISTINCT hash(client_id)) AS clients
FROM access
WHERE production AND year BETWEEN year(d0) AND year(d1) AND date BETWEEN d0 AND d1
GROUP BY ALL
ORDER BY day, era;
