-- The public traffic dashboard (seandavi/bioc-traffic#6). DuckDB, after sql/rollup_tier.sql,
-- whose `hits` view (production requests, client_class, client_key) both macros read, so a new
-- client_class version is picked up there and here at once:
--   just duckdb -c ".read sql/rollup_tier.sql" -c ".read sql/traffic.sql" \
--               -c "FROM traffic_day(DATE '2026-10-01', DATE '2026-10-01')"
--
-- `just traffic` keeps traffic_day.parquet (the forever series, private) and writes the public
-- traffic.json from it plus traffic_top_pages. Only aggregates: no c_ip, no client_id.

-- Web usage vs package usage: a package download on the fixed /packages/stats/ definition
-- (package_download_v0, which sets hits.package) is 'package', everything else 'web'.
CREATE OR REPLACE MACRO traffic_surface(package) AS CASE WHEN package IS NULL THEN 'web' ELSE 'package' END;

-- Requests and distinct clients per UTC day, era, surface and client_class; client_class NULL
-- is all classes (distinct clients do not add across classes, or across days and eras). era
-- stays a key, so the 2026-09-28 cutover is a seam in the data, not an average across it.
CREATE OR REPLACE MACRO traffic_day(d0, d1) AS TABLE
WITH per_client AS (
    SELECT date AS day, era, traffic_surface(package) AS surface, client_class, client_key,
           count(*) AS requests
    FROM hits
    WHERE year BETWEEN year(d0) AND year(d1) AND date BETWEEN d0 AND d1
    GROUP BY ALL
)
SELECT day, era, surface, client_class, client_class_rule_version() AS rule_version,
       CAST(sum(requests) AS BIGINT) AS requests, count(DISTINCT client_key) AS clients
FROM per_client
GROUP BY GROUPING SETS ((day, era, surface, client_class), (day, era, surface))
ORDER BY day, era, surface, client_class NULLS FIRST;

-- A page view for the top-pages table: a 200 for a document-shaped path (ends in '/', .html,
-- or no extension), so the CSS, JS and images every page load pulls in are left out.
CREATE OR REPLACE MACRO traffic_page_v0(uri, status) AS
    status = '200' AND regexp_matches(uri, '(/|\.html?|/[^/.]*)$');

-- Top pages by human_browser page views over [d0, d1] (UTC days). A page reached by fewer than
-- min_clients distinct clients is left out: it says little and could single someone out.
CREATE OR REPLACE MACRO traffic_top_pages(d0, d1, top_n := 50, min_clients := 10) AS TABLE
SELECT page, count(*) AS requests, count(DISTINCT client_key) AS clients
FROM hits
WHERE year BETWEEN year(d0) AND year(d1) AND date BETWEEN d0 AND d1
  AND client_class = 'human_browser' AND package IS NULL AND traffic_page_v0(page, status)
GROUP BY page
HAVING count(DISTINCT client_key) >= min_clients
ORDER BY requests DESC, page
LIMIT top_n;
