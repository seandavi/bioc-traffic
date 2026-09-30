-- The unified access view over both eras (DuckDB, #8). What #10 and #11 read.
--
--   cloudfront_access   CloudFront Parquet, 2020-01-01 .. 2026-08-05 (cloudfront-logs-to-parquet.py)
--   cloudflare_access   Logpush Parquet, hourly, from 2026-08-06 (cloudflare-logs-to-parquet.py)
--   access              both, UNION ALL: the 33 CloudFront columns, year, month, ts, era, client_id,
--                       production
--
-- No time cut between eras. After the 2026-09-28 cutover CloudFront keeps serving clients
-- with cached DNS, and before it the Worker served only a dev host, so a request is in
-- exactly one era. `production` separates the site from dev and stray hosts instead
-- (x_host_header, which both eras log: 'bioconductor.org', 'www.bioconductor.org', plus a
-- trailing dot or :8443 now and then). Stats filter on it; the rows stay (ADR 0002).
--
-- client_id = sha256(salt || c_ip), ADR 0012. Stored in the Cloudflare Parquet; computed
-- here for CloudFront with the same salt, so the eras share one ID space. Set the salt
-- first (`just duckdb` does), or anything that touches client_id errors out:
--   SET VARIABLE ip_salt = '<bioc-logs-ip-salt: its 64-char hex text, newline trimmed>';
--
-- Filter on year/month: they are the Hive partitions of both mirrors (ANALYTICS.md,
-- "Benchmarking traps"). The eras differ in how they say "missing": CloudFront logs '-',
-- the Worker logs NULL. Left as logged, as in the BigQuery view.

CREATE OR REPLACE VIEW cloudfront_access AS
SELECT *,
       date + CAST(time AS TIME) AS ts,
       'cloudfront' AS era,
       CASE WHEN getvariable('ip_salt') IS NULL
            THEN error('SET VARIABLE ip_salt first (see sql/access.sql)')
            ELSE sha256(getvariable('ip_salt') || c_ip) END AS client_id
FROM read_parquet('/data/davsean/bioc-cf-parquet/**/*.parquet', hive_partitioning = true);

CREATE OR REPLACE VIEW cloudflare_access AS
SELECT *, 'cloudflare' AS era
FROM read_parquet('/data/davsean/bioc-cloudflare-parquet/**/*.parquet', hive_partitioning = true);

CREATE OR REPLACE VIEW access AS
SELECT date, time, x_edge_location, sc_bytes, c_ip, cs_method, cs_host, cs_uri_stem, sc_status,
       cs_referer, cs_user_agent, cs_uri_query, cs_cookie, x_edge_result_type, x_edge_request_id,
       x_host_header, cs_protocol, cs_bytes, time_taken, x_forwarded_for, ssl_protocol, ssl_cipher,
       x_edge_response_result_type, cs_protocol_version, fle_status, fle_encrypted_fields, c_port,
       time_to_first_byte, x_edge_detailed_result_type, sc_content_type, sc_content_len,
       sc_range_start, sc_range_end, year, month, ts, era, client_id,
       regexp_matches(lower(x_host_header), '^(www\.)?bioconductor\.org\.?(:[0-9]+)?$') AS production
FROM cloudfront_access
UNION ALL
SELECT date, time, x_edge_location, sc_bytes, c_ip, cs_method, cs_host, cs_uri_stem, sc_status,
       cs_referer, cs_user_agent, cs_uri_query, cs_cookie, x_edge_result_type, x_edge_request_id,
       x_host_header, cs_protocol, cs_bytes, time_taken, x_forwarded_for, ssl_protocol, ssl_cipher,
       x_edge_response_result_type, cs_protocol_version, fle_status, fle_encrypted_fields, c_port,
       time_to_first_byte, x_edge_detailed_result_type, sc_content_type, sc_content_len,
       sc_range_start, sc_range_end, year, month, ts, era, client_id,
       regexp_matches(lower(x_host_header), '^(www\.)?bioconductor\.org\.?(:[0-9]+)?$') AS production
FROM cloudflare_access;
