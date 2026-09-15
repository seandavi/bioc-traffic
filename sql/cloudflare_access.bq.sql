WITH rec AS (
  SELECT m
  FROM `bioc-u24.logs.cf_workers_trace_raw`,
       UNNEST(Logs) AS l, UNNEST(l.Message) AS m
  WHERE JSON_VALUE(m, '$.type') = 'access'
),
t AS (
  SELECT m, TIMESTAMP_MILLIS(CAST(JSON_VALUE(m, '$.ts') AS INT64)) AS ts FROM rec
)
SELECT
  DATE(ts)                                   AS date,
  FORMAT_TIMESTAMP('%H:%M:%S', ts)           AS time,
  JSON_VALUE(m, '$.x_edge_location')         AS x_edge_location,
  JSON_VALUE(m, '$.sc_bytes')                AS sc_bytes,
  JSON_VALUE(m, '$.c_ip')                    AS c_ip,
  JSON_VALUE(m, '$.cs_method')               AS cs_method,
  JSON_VALUE(m, '$.cs_host')                 AS cs_host,
  JSON_VALUE(m, '$.cs_uri_stem')             AS cs_uri_stem,
  JSON_VALUE(m, '$.sc_status')               AS sc_status,
  JSON_VALUE(m, '$.cs_referer')              AS cs_referer,
  JSON_VALUE(m, '$.cs_user_agent')           AS cs_user_agent,
  -- Worker logs url.search ("?a=b"); CloudFront logs the bare query. Strip the "?".
  NULLIF(LTRIM(JSON_VALUE(m, '$.cs_uri_query'), '?'), '') AS cs_uri_query,
  JSON_VALUE(m, '$.cs_cookie')               AS cs_cookie,
  JSON_VALUE(m, '$.x_edge_result_type')      AS x_edge_result_type,
  JSON_VALUE(m, '$.x_edge_request_id')       AS x_edge_request_id,
  JSON_VALUE(m, '$.x_host_header')           AS x_host_header,
  JSON_VALUE(m, '$.cs_protocol')             AS cs_protocol,
  JSON_VALUE(m, '$.cs_bytes')                AS cs_bytes,
  -- Worker records milliseconds; CloudFront's column is seconds ("0.002").
  IF(JSON_VALUE(m, '$.time_taken') IS NULL, NULL,
     FORMAT('%.3f', CAST(JSON_VALUE(m, '$.time_taken') AS INT64) / 1000)) AS time_taken,
  JSON_VALUE(m, '$.x_forwarded_for')         AS x_forwarded_for,
  JSON_VALUE(m, '$.ssl_protocol')            AS ssl_protocol,
  JSON_VALUE(m, '$.ssl_cipher')              AS ssl_cipher,
  JSON_VALUE(m, '$.x_edge_response_result_type') AS x_edge_response_result_type,
  JSON_VALUE(m, '$.cs_protocol_version')     AS cs_protocol_version,
  JSON_VALUE(m, '$.fle_status')              AS fle_status,
  JSON_VALUE(m, '$.fle_encrypted_fields')    AS fle_encrypted_fields,
  JSON_VALUE(m, '$.c_port')                  AS c_port,
  JSON_VALUE(m, '$.time_to_first_byte')      AS time_to_first_byte,
  JSON_VALUE(m, '$.x_edge_detailed_result_type') AS x_edge_detailed_result_type,
  JSON_VALUE(m, '$.sc_content_type')         AS sc_content_type,
  JSON_VALUE(m, '$.sc_content_len')          AS sc_content_len,
  JSON_VALUE(m, '$.sc_range_start')          AS sc_range_start,
  JSON_VALUE(m, '$.sc_range_end')            AS sc_range_end,
  EXTRACT(YEAR FROM ts)                      AS year,
  EXTRACT(MONTH FROM ts)                     AS month
FROM t
