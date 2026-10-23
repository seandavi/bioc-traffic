-- client_class v1 — per-request traffic class, both eras (seandavi/bioc-traffic#5).
--
-- Load after sql/access.sql, sql/client_class.sql (v0, whose rules 1–11 v1 reuses unchanged)
-- and sql/ipref.sql (`just duckdb` does):
--   FROM classified_access_v1(DATE '2026-10-22', DATE '2026-10-22')
-- Check:  duckdb -c ".read sql/ipref.sql" -c ".read sql/client_class.sql" \
--                -c ".read sql/client_class_v1.sql" -c ".read sql/client_class_v1_check.sql"
--
-- classified_access_v1(d0, d1, src := 'access') is every row of src in the UTC days d0..d1,
-- plus client_class and rule_version, like classified_access_v0 (sql/client_class.sql): to
-- switch a consumer between versions, change the macro name. src needs access's columns.
--
-- What v1 adds to v0, for browser-shaped user agents only (v0 rule 12; every other UA keeps its
-- v0 class):
--   - ASN in both eras: cf_asn where Cloudflare logged it, else DB-IP's for the address
--     (dbip_release_for the request's date; #4). v0 had ASN only in the Cloudflare era.
--   - The published crawler ranges (crawler_fetch_for the date): the CloudFront-era stand-in
--     for cf.verifiedBotCategory. Also applied to v0's 'unknown'.
--   - A longer hosting-ASN list (client_hosting_asn_v1).
--   - Behaviour per UTC day of the address's and its /24's (/48 for IPv6) browser-shaped
--     requests (client_behaviour_v1). Whole days, so a row's class does not depend on the
--     query window: pass d0..d1 covering it and filter ts afterwards.
--
-- Known ceilings: a crawler rotating through residential addresses at one or two requests
-- each, spread thinly over /24s, still looks like people (10-22: ~100k such addresses in
-- Chinanet, VNPT, Viettel, Turk Telekom… at 1.2–2 requests and <10% assets each; the
-- dense ones are caught by the /24 rule). VPN egress on hosting ASNs is people classed as
-- likely_automated, as in v0. CloudFront-era rows get the earliest DB-IP release and crawler
-- fetch (2026-09, 2026-10-23), so ranges that moved since are misattributed.

-- Datacenter, VPS, colo and proxy-leasing ASNs seen sending browser user agents on 2026-07-15
-- (CloudFront) or 2026-10-22 (Cloudflare), on top of v0's. Not added, though they host: the
-- relays that carry people (Zscaler, Netskope, Cloudflare WARP 13335, iCloud Private Relay on
-- Akamai 36183), corporate egress (Microsoft 8070, Google 394089), transit (Cogent 174,
-- Hurricane Electric 6939, GTT 3257).
CREATE OR REPLACE MACRO client_hosting_asn_v1(asn) AS client_hosting_asn_v0(asn) OR asn IN (
    55967,                   -- Baidu Netcom: 120 addresses in one /24, browser UAs, all 24 h
    21859, 135377, 906, 63150,                 -- Zenlayer, UCloud HK, DMIT, BAGE Cloud
    197540, 214996, 57433, 208208, 213881,     -- netcup, intercolo, Cyberse, OUR Technology
    11878, 210644, 206996, 63473, 50131, 201106,  -- tzulo, AEZA, ZAP-Hosting, HostHatch, Spartan
    18779, 36352, 63023, 51765, 40065, 26548,  -- EGIHosting, HostPapa, GTHost, Crea Nova, CNSERVERS, PureVoltage
    201217, 16125, 214159, 44559, 40676, 14315,   -- RadishCloud, Cherry Servers, IT Hostline, Psychz, 1GSERVERS
    197860, 215607, 198550, 218785, 203273,    -- Haruka, dataforest, NODE HOST, TC Datacenter, NetCrafters
    201279, 215590, 49791, 48282, 213230,      -- Loginov, DpkgSoft, Newserverlife, Hosting technology, Hetzner
    202015, 26277, 138362, 51396, 19318,       -- HZ Hosting, Las Vegas NV Datacenter, Plexus, Pfcloud, Interserver
    395092, 263735, 46997, 11798, 401152, 55286,  -- Shock, Buena Hosting, Black Mesa, Ace Data Centers, B2 Net
    395954, 393886, 7203, 60781, 59253, 134351, 133752,  -- Leaseweb
    207990, 133499, 134450,                    -- HostRoyale
    -- Proxy-leasing blocks: thousands of addresses at ~1.3 requests each, <10% assets.
    62874, 46635, 401560, 214669, 216067       -- Web2Objects, AviationAI, OneCable, Starlight, BHS
);

-- v0 rule 12's shape. u is client_ua_v0(ua).
CREATE OR REPLACE MACRO client_browser_ua_v1(u) AS
    regexp_matches(u, '^(mozilla|opera)/')
    AND regexp_matches(u, 'chrome/|firefox/|safari/|edg/|opr/|msie |trident/|crios/|fxios/');

-- Page assets: what a browser fetches with a page and a page-only crawler does not.
CREATE OR REPLACE MACRO client_asset_v1(uri) AS
    regexp_matches(lower(uri), '\.(css|js|map|png|jpe?g|gif|svg|ico|webp|woff2?|ttf)$');

-- The /24 (IPv4) or /48 (IPv6) as a number within its version.
CREATE OR REPLACE MACRO client_net_v1(ip) AS
    ip_num(ip) >> CASE ip_version(ip) WHEN 4 THEN 8 ELSE 80 END::UHUGEINT;

-- Behaviour per (date, c_ip), from src's browser-shaped requests that day:
--   addr_pages        distinct non-asset paths the address requested
--   addr_hours        distinct UTC hours it was active in
--   net_addrs         distinct addresses in its /24 or /48
--   net_asset_share   the /24's or /48's share of requests that were assets
-- Pure SQL over src, no lookups, so sql/client_class_v1_check.sql can test it.
CREATE OR REPLACE MACRO client_behaviour_v1(d0, d1, src := 'access') AS TABLE
WITH b AS (
    SELECT date, c_ip, hour(ts) AS h, cs_uri_stem, client_asset_v1(cs_uri_stem) AS asset
    FROM query_table(src)
    WHERE access_in_days_(year, month, date, d0, d1)
      AND client_browser_ua_v1(client_ua_v0(cs_user_agent))
),
addr AS (
    SELECT *, ip_version(c_ip) AS v, client_net_v1(c_ip) AS net
    FROM (SELECT date, c_ip,
                 count(DISTINCT cs_uri_stem) FILTER (WHERE NOT asset) AS addr_pages,
                 count(DISTINCT h) AS addr_hours,
                 count(*) AS requests, count(*) FILTER (WHERE asset) AS assets
          FROM b GROUP BY date, c_ip)
)
SELECT date, c_ip, addr_pages, addr_hours,
       count(*) OVER w AS net_addrs, sum(assets) OVER w / sum(requests) OVER w AS net_asset_share
FROM addr
WINDOW w AS (PARTITION BY date, v, net);

-- ASN and published-crawler class per (date, c_ip): cf_asn if logged, else DB-IP's.
-- Needs ipref.py's data (sql/ipref.sql).
-- DuckDB estimates the grouped addresses below at one row, so by default it plans the ASOF
-- join as a nested loop against all of DB-IP: 2026-10-22 took over 2 min instead of 10 s.
SET asof_loop_join_threshold = 0;
CREATE OR REPLACE MACRO client_address_v1(d0, d1, src := 'access') AS TABLE
WITH a AS (
    SELECT date, c_ip, any_value(cf_asn) AS cf_asn
    FROM query_table(src)
    WHERE access_in_days_(year, month, date, d0, d1)
    GROUP BY date, c_ip
),
days AS (
    SELECT date, dbip_release_for(date) AS release, CAST(crawler_fetch_for(date) AS DATE) AS crawler_fetch
    FROM (SELECT DISTINCT date FROM a)
),
ip AS (
    SELECT a.*, release, crawler_fetch, ip_version(c_ip) AS v, ip_num(c_ip) AS n
    FROM a JOIN days USING (date)
),
crawler AS (
    -- Lists overlap only within a class (gptbot, searchbot), so min() picks nothing arbitrary.
    SELECT ip.date, ip.c_ip, min(CASE WHEN r.operator = 'google' AND r.list LIKE 'user-triggered%'
                                      THEN 'likely_automated'
                                      WHEN r.operator IN ('google', 'bing') THEN 'search_crawler'
                                      ELSE 'ai_crawler' END) AS crawler_class
    FROM ip JOIN crawler_ranges('*') r
      ON r.fetched = ip.crawler_fetch AND r.ip_version = ip.v AND ip.n BETWEEN r.ip_first AND r.ip_last
    GROUP BY ip.date, ip.c_ip
)
SELECT ip.date, ip.c_ip, coalesce(ip.cf_asn, r.asn) AS asn, crawler.crawler_class
FROM ip
ASOF LEFT JOIN dbip('*') r
  ON ip.release = r.dbip_release AND ip.v = r.ip_version AND ip.n >= r.ip_first
LEFT JOIN crawler USING (date, c_ip);

-- The rules, given the features. base is client_class_v0_(u, uri, 'GET', bot_category, NULL):
-- v0 with method and ASN withheld, so v0's rule 12 answers human_browser for every
-- browser-shaped UA its rules 1–11 let through. A column, not inlined three times here: that
-- tripled v0's cost, 45 s of CPU for 2026-07-15. Thresholds,
-- from the addresses that reach them (a browser UA, no hosting ASN or crawler range) on
-- 2026-07-15 (CloudFront, 394k addresses) and 2026-10-22 (Cloudflare, 376k):
--   addr_pages >= 200: university, hospital and research-network addresses reached 108
--     distinct pages (a NAT for many people); 99.9% of all stay under 20. 11 and 4 addresses
--     at 200+, no institution, most also active 12+ hours.
--   addr_hours >= 12: institution addresses were active in at most 10 UTC hours. 183 and 98
--     addresses at 12+, bunched at 22–24 hours: steady pollers.
--   net_addrs >= 50: no institution /24 or /48 reached 20 browser addresses (MD Anderson: 15)
--     and no carrier CGNAT /24 50 (T-Mobile: 35). At 50+: proxy pools (504 and 643 nets with
--     asset share < 0.5) and headless renderers (10-22: 18 Cox-labelled /24s, 4 UAs between
--     them, 71k requests at share 0.69).
--   net_addrs >= 20 with net_asset_share < 0.5, phones exempt: residential-proxy pools at
--     1–2 page-only requests per address, mostly one desktop UA each (10-22: Mac Chrome 132
--     over 39k Chinanet addresses, Mac Chrome 142 over 11k Cox and 6k GTT). Carrier CGNAT
--     /24s hold dozens of phones with cached assets: without the exemption, 10-22's 4.3k
--     T-Mobile, AT&T, Starlink… addresses on iPhone and Android UAs (8k requests) were
--     caught; 07-15's phones, 46 requests. A pool that sends phone UAs gets through.
CREATE OR REPLACE MACRO client_class_v1_(u, base, method, asn, crawler_class,
                                         addr_pages, addr_hours, net_addrs, net_asset_share) AS
CASE
    WHEN base NOT IN ('human_browser', 'unknown') THEN base
    -- A published crawler address. Not before the UA rules: R from Google's user-triggered
    -- ranges (Colab, 11k requests on 10-22) is a person's package client.
    WHEN crawler_class IS NOT NULL THEN crawler_class
    WHEN base = 'unknown' THEN 'unknown'
    WHEN client_hosting_asn_v1(asn) OR method = 'HEAD' THEN 'likely_automated'
    WHEN addr_pages >= 200 OR addr_hours >= 12 OR net_addrs >= 50
      OR (net_addrs >= 20 AND net_asset_share < 0.5
          AND NOT regexp_matches(u, 'mobile|android|iphone|ipad')) THEN 'likely_automated'
    ELSE 'human_browser'
END;

CREATE OR REPLACE MACRO classified_access_v1(d0, d1, src := 'access') AS TABLE
SELECT a.* EXCLUDE (u, base),
       client_class_v1_(a.u, a.base, a.cs_method, ad.asn, ad.crawler_class, b.addr_pages,
                        b.addr_hours, b.net_addrs, b.net_asset_share) AS client_class,
       'v1' AS rule_version
FROM (SELECT *, client_class_v0_(u, cs_uri_stem, 'GET', bot_category, NULL) AS base
      FROM (SELECT *, client_ua_v0(cs_user_agent) AS u FROM query_table(src)
            WHERE access_in_days_(year, month, date, d0, d1))) a
LEFT JOIN client_address_v1(d0, d1, src := src) ad USING (date, c_ip)
LEFT JOIN client_behaviour_v1(d0, d1, src := src) b USING (date, c_ip);
