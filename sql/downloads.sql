-- Package downloads over both eras (DuckDB, #11). Load after sql/access.sql and
-- sql/client_class.sql (`just duckdb` loads all three).
--
-- The row filter is DOWNLOADS_SQL in cloudfront-logs-to-parquet.py, verbatim: a tarball or
-- binary under /packages/, status 200/301/302/307/308, HEAD excluded. It reproduces the
-- published bioc figures, so it is the *fixed* definition; keep PKG_URI identical to the
-- Python copy (download-stats.py --self-check asserts it). sc_status is VARCHAR in both eras.
--
-- `production` is a column, not a filter here: the stats filter on it (dev traffic on
-- bioc-dev.cancerdatasci.org hits the same paths). client_class is interpretive and
-- carries its rule version; the Cloudflare era feeds it cf_asn and bot_category, the
-- CloudFront era cannot, so the human/automated split steps at the cutover (#11).
--
--   category      bioc, data-annotation, data-experiment, workflows (the /packages/stats/ dirs)
--   bioc_version  the /packages/<x>/ segment: '3.22', or an alias such as 'release', as requested
CREATE OR REPLACE VIEW downloads AS
SELECT ts, date, year, month, era, client_id, production,
       replace(regexp_replace(
           regexp_extract(cs_uri_stem, '^/+packages/+[^/]+/+(bioc|workflows|data/+experiment|data/+annotation)/+(bin|src)/+.*_.*\.(tar\.gz|zip|tgz)$', 1),
           '/+', '/', 'g'), '/', '-') AS category,
       regexp_extract(cs_uri_stem, '/([^/_]+)_[^/]*\.(tar\.gz|zip|tgz)$', 1) AS package,
       regexp_extract(cs_uri_stem, '^/+packages/+([^/]+)/', 1) AS bioc_version,
       client_class_v0(cs_user_agent, cs_uri_stem, cs_method,
                       bot_category := bot_category, asn := cf_asn) AS client_class,
       client_class_rule_version() AS rule_version
FROM access
WHERE sc_status IN ('200','301','302','307','308')
  AND cs_method <> 'HEAD'
  AND regexp_matches(cs_uri_stem, '^/+packages/+[^/]+/+(bioc|workflows|data/+experiment|data/+annotation)/+(bin|src)/+.*_.*\.(tar\.gz|zip|tgz)$');
