-- sql/traffic.sql checks. Fails (non-zero exit) on the first mismatch. Needs the logs (~10 s):
--   just duckdb -c ".read sql/rollup_tier.sql" -c ".read sql/rollup_overall.sql" \
--               -c ".read sql/traffic.sql" -c ".read sql/traffic_check.sql"

WITH cases(uri, status, expected) AS (VALUES
    ('/', '200', true), ('/about/', '200', true), ('/install', '200', true),
    ('/packages/release/bioc/html/limma.html', '200', true),
    ('/packages/release/bioc/vignettes/DESeq2/inst/doc/DESeq2.html', '200', true),
    ('/js/bioconductor.js', '200', false), ('/style/base/layout.css', '200', false),
    ('/favicon.ico', '200', false), ('/packages/3.22/bioc/src/contrib/PACKAGES.gz', '200', false),
    ('/about/', '301', false), ('/about/', '404', false)
)
SELECT CASE WHEN count(*) FILTER (WHERE traffic_page_v0(uri, status) IS DISTINCT FROM expected) = 0
             AND traffic_surface(NULL) = 'web' AND traffic_surface('limma') = 'package'
            THEN 'traffic_page_v0 / traffic_surface: ' || count(*) || ' cases ok'
            ELSE error('traffic_page_v0 mismatch: ' || string_agg(uri || ' ' || status, '; ')
                       FILTER (WHERE traffic_page_v0(uri, status) IS DISTINCT FROM expected)) END AS result
FROM cases;

-- The cutover day has both eras. Per era: web + package = every production request
-- (rollup_overall_day, which reads `access`, not `hits`), and the classes sum to the total.
CREATE TEMP TABLE d AS FROM traffic_day(DATE '2026-09-28', DATE '2026-09-28');
WITH o AS (FROM rollup_overall_day(DATE '2026-09-28', DATE '2026-09-28')),
t AS (SELECT era, sum(requests) FILTER (WHERE client_class IS NULL) AS total,
             sum(requests) FILTER (WHERE client_class IS NOT NULL) AS by_class
      FROM d GROUP BY era)
SELECT CASE WHEN (SELECT count(*) FROM o) = 2
             AND (SELECT count(*) FROM o JOIN t USING (era)
                  WHERE o.requests = t.total AND t.total = t.by_class) = 2
             AND (SELECT count(DISTINCT rule_version) = 1 AND min(rule_version) = client_class_rule_version() FROM d)
            THEN 'traffic_day 2026-09-28: both eras, totals = rollup_overall_day, classes add up'
            ELSE error('traffic_day 2026-09-28 does not reconcile with rollup_overall_day') END AS result;
