-- DB-IP (sql/ipref.sql) against Cloudflare's own cf_asn and cf_country, one UTC day of the
-- Cloudflare era (#4), with the DB-IP release in effect that day. Reports, does not fail.
--   just ipref-agreement 2026-10-21
-- Agreement is over rows (requests) and distinct addresses where Cloudflare has a value.
-- Cloudflare's 'XX' (unknown) and 'T1' (Tor) count as disagreements; DB-IP has neither.
SET VARIABLE release = dbip_release_for(getvariable('day'));

CREATE OR REPLACE TEMP TABLE ipref_agreement AS
WITH ips AS (
    SELECT c_ip, cf_asn, cf_country, count(*) AS requests
    FROM cloudflare_access
    WHERE year = year(getvariable('day')) AND month = month(getvariable('day'))
      AND day = day(getvariable('day'))
    GROUP BY ALL
)
SELECT i.*, ip_version(i.c_ip) AS ip_version, r.dbip_release, r.asn, r.as_org, r.country
FROM ips i
ASOF LEFT JOIN dbip(getvariable('release')) r
  ON ip_version(i.c_ip) = r.ip_version AND ip_num(i.c_ip) >= r.ip_first;

SELECT getvariable('day') AS day, any_value(dbip_release) AS dbip_release,
       coalesce('IPv' || ip_version, 'all') AS addresses,
       sum(requests) AS requests, count(DISTINCT c_ip) AS distinct_ips,
       round(100 * sum(requests) FILTER (WHERE asn = cf_asn)
             / sum(requests) FILTER (WHERE cf_asn IS NOT NULL), 2) AS asn_agree_pct,
       round(100 * count(DISTINCT c_ip) FILTER (WHERE asn = cf_asn)
             / count(DISTINCT c_ip) FILTER (WHERE cf_asn IS NOT NULL), 2) AS asn_agree_pct_ips,
       round(100 * sum(requests) FILTER (WHERE country = cf_country)
             / sum(requests) FILTER (WHERE cf_country IS NOT NULL), 2) AS country_agree_pct,
       round(100 * count(DISTINCT c_ip) FILTER (WHERE country = cf_country)
             / count(DISTINCT c_ip) FILTER (WHERE cf_country IS NOT NULL), 2) AS country_agree_pct_ips,
       round(100 * sum(requests) FILTER (WHERE asn IS NULL) / sum(requests), 2) AS dbip_no_asn_pct
FROM ipref_agreement
GROUP BY GROUPING SETS ((ip_version), ())
ORDER BY addresses;

-- Where they differ most, by requests
SELECT cf_asn, asn AS dbip_asn, any_value(as_org) AS dbip_as_org, sum(requests) AS requests,
       count(DISTINCT c_ip) AS distinct_ips
FROM ipref_agreement
WHERE cf_asn IS DISTINCT FROM asn
GROUP BY ALL ORDER BY requests DESC LIMIT 10;
