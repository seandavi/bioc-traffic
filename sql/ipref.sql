-- ASN and country by address for both eras, and the crawlers' published ranges (DuckDB, #4).
-- Data written by ipref.py. Load: .read sql/ipref.sql (`just duckdb` does). Needs no data to load.
--
--   ip_version(ip)          4 or 6 (an IPv4-mapped '::ffff:1.2.3.4' is 4); NULL if neither
--   ip_num(ip)              the address as a number within its version, UHUGEINT; NULL if malformed
--   dbip(release)           one DB-IP Lite release ('2026-10'; '*' for all): dbip_release,
--                           ip_version, ip_first, ip_last, asn, as_org, country. Non-overlapping
--                           ranges covering each address space, NULLs where DB-IP has nothing,
--                           so one ASOF join is an exact lookup.
--   dbip_release_for(d)     the release in effect on date d: the latest at or before d's month,
--                           else the earliest (CloudFront-era dates predate every release)
--   crawler_ranges(date)    Google, Bing, OpenAI, Anthropic published prefixes as fetched on
--                           date ('2026-10-23'; '*' for every fetch): fetched, operator, list,
--                           creation_time, prefix, ip_version, ip_first, ip_last. CIDRs, so
--                           match with BETWEEN ip_first AND ip_last (lists can overlap)
--   crawler_fetch_for(d)    the fetch in effect on date d, as dbip_release_for: the latest at or
--                           before d, else the earliest
--
-- Lookup, either era:
--   SELECT a.c_ip, r.asn, r.as_org, r.country
--   FROM access a
--   ASOF LEFT JOIN dbip('2026-10') r
--     ON ip_version(a.c_ip) = r.ip_version AND ip_num(a.c_ip) >= r.ip_first
--   WHERE a.year = 2026 AND a.month = 10
-- Join on DISTINCT addresses first when the rows are many: the address is the whole key.
-- The two versions are separate number spaces: DB-IP's country file covers ::/3 itself, so
-- IPv4 cannot live at ::ffff:0:0/96 without colliding.
--
-- Data: DB-IP Lite (https://db-ip.com), CC BY 4.0. Anything published from these columns
-- carries "IP Geolocation by DB-IP" with that link (THIRD_PARTY_NOTICES).

CREATE OR REPLACE MACRO ipref_root() AS coalesce(getvariable('ipref_root'), '/data/davsean/bioc-traffic-ipref');

CREATE OR REPLACE MACRO ip_version(ip) AS
    CASE WHEN contains(ip, '.') THEN 4 WHEN contains(ip, ':') THEN 6 END;

-- IPv6 text as its eight 16-bit groups: '::' stands for as many zero groups as are missing.
CREATE OR REPLACE MACRO ip6_groups_(ip) AS CASE WHEN contains(ip, '::') THEN list_concat(
        list_filter(string_split(split_part(ip, '::', 1), ':'), lambda g: g <> ''),
        list_transform(range(8 - len(list_filter(string_split(ip, ':'), lambda g: g <> ''))),
                       lambda _: '0'),
        list_filter(string_split(split_part(ip, '::', 2), ':'), lambda g: g <> ''))
    ELSE string_split(ip, ':') END;

CREATE OR REPLACE MACRO ip_num(ip) AS CASE ip_version(ip)
    WHEN 4 THEN CASE WHEN len(string_split(regexp_extract(ip, '[0-9.]+$'), '.')) = 4 THEN list_reduce(
        list_transform(string_split(regexp_extract(ip, '[0-9.]+$'), '.'),
                       lambda o: CASE WHEN TRY_CAST(o AS UTINYINT) IS NOT NULL THEN o::UHUGEINT END),
        lambda a, b: a * 256 + b) END
    WHEN 6 THEN CASE WHEN len(ip6_groups_(ip)) = 8 THEN list_reduce(
        list_transform(ip6_groups_(ip),
                       lambda g: CASE WHEN length(g) <= 4 THEN TRY_CAST('0x' || g AS UHUGEINT) END),
        lambda a, b: a * 65536 + b) END
END;

-- Parquet has no 128-bit integer (DuckDB writes UHUGEINT as DOUBLE), so ipref.py stores each
-- address as two UBIGINT halves.
CREATE OR REPLACE MACRO ip_join_(hi, lo) AS (hi::UHUGEINT << 64) | lo;

CREATE OR REPLACE MACRO dbip(release) AS TABLE
SELECT dbip_release, ip_version, ip_join_(first_hi, first_lo) AS ip_first,
       ip_join_(last_hi, last_lo) AS ip_last, asn, as_org, country
FROM read_parquet(ipref_root() || '/dbip/dbip-' || release || '.parquet');

CREATE OR REPLACE MACRO dbip_release_for(d) AS (
    SELECT coalesce(max(r) FILTER (WHERE r <= strftime(CAST(d AS DATE), '%Y-%m')), min(r))
    FROM (SELECT regexp_extract(file, 'dbip-([0-9]{4}-[0-9]{2})\.parquet$', 1) AS r
          FROM glob(ipref_root() || '/dbip/dbip-*.parquet')));

CREATE OR REPLACE MACRO crawler_fetch_for(d) AS (
    SELECT coalesce(max(f) FILTER (WHERE f <= strftime(CAST(d AS DATE), '%Y-%m-%d')), min(f))
    FROM (SELECT regexp_extract(file, 'crawlers-([0-9]{4}-[0-9]{2}-[0-9]{2})\.parquet$', 1) AS f
          FROM glob(ipref_root() || '/crawlers/crawlers-*.parquet')));

CREATE OR REPLACE MACRO crawler_ranges(date) AS TABLE
SELECT fetched, operator, list, creation_time, prefix, ip_version,
       ip_join_(first_hi, first_lo) AS ip_first, ip_join_(last_hi, last_lo) AS ip_last
FROM read_parquet(ipref_root() || '/crawlers/crawlers-' || date || '.parquet');
