-- Known cases for client_class v1 and classified_access_v0/v1. Fails (non-zero exit) on the
-- first mismatch. The last block looks addresses up, so it needs `just ipref`'s data:
--   duckdb -c ".read sql/ipref.sql" -c ".read sql/client_class.sql" \
--          -c ".read sql/client_class_v1.sql" -c ".read sql/client_class_v1_check.sql"

-- The #5 disguised crawler's user agent (Mac Chrome 136), also a person's.
SET VARIABLE chrome = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36';

-- The rules, given the features: each threshold just above and just below.
WITH cases(ua, uri, method, bot_category, asn, crawler_class, addr_pages, addr_hours, net_addrs,
           net_asset_share, expected) AS (VALUES
    -- Hosting ASNs: v0's list (the #5 crawler, both eras now) and v1's additions
    (getvariable('chrome'), '/help/course-materials/', 'GET', '', 150436, NULL, 1, 1, 1, 1.0, 'likely_automated'),
    (getvariable('chrome'), '/', 'GET', NULL, 57433, NULL, 1, 1, 1, 1.0, 'likely_automated'),
    -- A person, at the largest values seen for people: university NAT, CGNAT /24
    (getvariable('chrome'), '/packages/release/bioc/html/limma.html', 'GET', NULL, 7922, NULL, 199, 11, 49, 0.5, 'human_browser'),
    (getvariable('chrome'), '/', 'GET', NULL, 7922, NULL, 200, 1, 1, 1.0, 'likely_automated'),
    (getvariable('chrome'), '/', 'GET', NULL, 7922, NULL, 1, 12, 1, 1.0, 'likely_automated'),
    (getvariable('chrome'), '/', 'GET', NULL, 7922, NULL, 1, 1, 50, 1.0, 'likely_automated'),
    (getvariable('chrome'), '/', 'GET', NULL, 7922, NULL, 1, 1, 20, 0.49, 'likely_automated'),
    (getvariable('chrome'), '/', 'GET', NULL, 7922, NULL, 1, 1, 19, 0.0, 'human_browser'),
    -- Phones behind a carrier's CGNAT /24, assets cached: exempt from the asset-share rule
    ('Mozilla/5.0 (iPhone; CPU iPhone OS 18_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Mobile/15E148 Safari/604.1', '/', 'GET', NULL, 21928, NULL, 1, 1, 35, 0.0, 'human_browser'),
    ('Mozilla/5.0 (iPhone; CPU iPhone OS 18_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Mobile/15E148 Safari/604.1', '/', 'GET', NULL, 21928, NULL, 1, 1, 50, 0.0, 'likely_automated'),
    (getvariable('chrome'), '/', 'GET', NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'human_browser'),
    (getvariable('chrome'), '/', 'HEAD', NULL, 7922, NULL, 1, 1, 1, 1.0, 'likely_automated'),
    -- v0's rules 1–11 first: Cloudflare's label, the /talks/ path, R from Colab's ranges
    (getvariable('chrome'), '/', 'GET', 'AI Crawler', 7922, NULL, 1, 1, 1, 1.0, 'ai_crawler'),
    (getvariable('chrome'), '/talks/2014/SeattleOct2014/2014/SeattleOct2014/x.Rmd', 'GET', NULL, 7922, NULL, 1, 1, 1, 0.0, 'likely_automated'),
    ('R (4.6.1 x86_64-pc-linux-gnu x86_64 linux-gnu)', '/packages/3.22/bioc/src/contrib/PACKAGES', 'GET', NULL, 15169, 'likely_automated', NULL, NULL, NULL, NULL, 'package_client'),
    -- Published crawler ranges, then hosting: a Googlebot address rendering with Chrome
    (getvariable('chrome'), '/', 'GET', NULL, 15169, 'search_crawler', 1, 1, 1, 1.0, 'search_crawler'),
    ('msh-pdfmin/1', '/', 'GET', NULL, 45102, 'ai_crawler', NULL, NULL, NULL, NULL, 'ai_crawler'),
    ('msh-pdfmin/1', '/', 'GET', NULL, 45102, NULL, NULL, NULL, NULL, NULL, 'unknown')
),
got AS (
    SELECT *, client_class_v1_(client_ua_v0(ua),
                               client_class_v0_(client_ua_v0(ua), uri, 'GET', bot_category, NULL),
                               method, asn, crawler_class, addr_pages, addr_hours, net_addrs,
                               net_asset_share) AS actual
    FROM cases
)
SELECT CASE WHEN count(*) FILTER (WHERE actual IS DISTINCT FROM expected) = 0
            THEN 'client_class_v1_: ' || count(*) || ' cases ok'
            ELSE error('client_class_v1_ mismatch: ' || string_agg(ua || ' ' || uri || ' → '
                       || coalesce(actual, 'NULL') || ' (expected ' || expected || ')', '; ')
                       FILTER (WHERE actual IS DISTINCT FROM expected)) END AS result
FROM got;

-- Access-shaped rows. 10.0.0.0/24 and 2001:db8:1::/48 hold two browser addresses each; the R
-- request and the next day's row must not count. 45.78.192.10 is BytePlus (DB-IP, AS150436),
-- 66.249.66.1 Googlebot, 23.24.1.1 Comcast: CloudFront-shaped, so no cf_asn.
CREATE OR REPLACE TEMP TABLE client_class_v1_check_access AS
SELECT CAST(ts AS DATE) AS date, year(ts) AS year, month(ts) AS month, *
FROM (VALUES
    (TIMESTAMP '2026-07-15 00:10', '10.0.0.1', '/a', getvariable('chrome'), 'GET', NULL::VARCHAR, NULL::BIGINT),
    (TIMESTAMP '2026-07-15 01:10', '10.0.0.1', '/a', getvariable('chrome'), 'GET', NULL, NULL),
    (TIMESTAMP '2026-07-15 01:20', '10.0.0.1', '/b', getvariable('chrome'), 'GET', NULL, NULL),
    (TIMESTAMP '2026-07-15 01:20', '10.0.0.1', '/style/x.css', getvariable('chrome'), 'GET', NULL, NULL),
    (TIMESTAMP '2026-07-15 05:00', '10.0.0.2', '/c', getvariable('chrome'), 'GET', NULL, NULL),
    (TIMESTAMP '2026-07-15 05:00', '10.0.0.2', '/x.JS', getvariable('chrome'), 'GET', NULL, NULL),
    (TIMESTAMP '2026-07-15 05:00', '10.0.0.3', '/packages/3.22/bioc/src/contrib/PACKAGES',
     'R (4.6.1 x86_64-pc-linux-gnu x86_64 linux-gnu)', 'GET', NULL, NULL),
    (TIMESTAMP '2026-07-15 06:00', '10.0.1.1', '/d.png', getvariable('chrome'), 'GET', NULL, NULL),
    (TIMESTAMP '2026-07-15 07:00', '2001:db8:1:aaaa::1', '/e', getvariable('chrome'), 'GET', NULL, NULL),
    (TIMESTAMP '2026-07-15 07:00', '2001:db8:1:bbbb::2', '/e', getvariable('chrome'), 'GET', NULL, NULL),
    (TIMESTAMP '2026-07-15 07:00', '2001:db8:2::1', '/e', getvariable('chrome'), 'GET', NULL, NULL),
    (TIMESTAMP '2026-07-16 00:00', '10.0.0.1', '/f', getvariable('chrome'), 'GET', NULL, NULL),
    (TIMESTAMP '2026-07-20 00:00', '45.78.192.10', '/help/course-materials/', getvariable('chrome'), 'GET', NULL, NULL),
    (TIMESTAMP '2026-07-20 00:00', '66.249.66.1', '/', getvariable('chrome'), 'GET', NULL, NULL),
    (TIMESTAMP '2026-07-20 00:00', '23.24.1.1', '/', getvariable('chrome'), 'GET', NULL, NULL),
    (TIMESTAMP '2026-07-20 00:00', '23.24.1.1', '/packages/3.22/bioc/src/contrib/PACKAGES',
     'R (4.6.1 x86_64-pc-linux-gnu x86_64 linux-gnu)', 'GET', NULL, NULL)
) t(ts, c_ip, cs_uri_stem, cs_user_agent, cs_method, bot_category, cf_asn);

WITH expected(c_ip, addr_pages, addr_hours, net_addrs, net_asset_share) AS (VALUES
    ('10.0.0.1', 2, 2, 2, 0.333), ('10.0.0.2', 1, 1, 2, 0.333), ('10.0.1.1', 0, 1, 1, 1.0),
    ('2001:db8:1:aaaa::1', 1, 1, 2, 0.0), ('2001:db8:1:bbbb::2', 1, 1, 2, 0.0),
    ('2001:db8:2::1', 1, 1, 1, 0.0)
),
got AS (
    SELECT c_ip, addr_pages, addr_hours, net_addrs, round(net_asset_share, 3) AS net_asset_share
    FROM client_behaviour_v1(DATE '2026-07-15', DATE '2026-07-15', src := 'client_class_v1_check_access')
),
diff AS (FROM (FROM expected EXCEPT FROM got) UNION ALL FROM (FROM got EXCEPT FROM expected))
SELECT CASE WHEN count(*) = 0 THEN 'client_behaviour_v1 ok'
            ELSE error('client_behaviour_v1 mismatch: ' || string_agg(d::VARCHAR, '; ')) END AS result
FROM diff d;

SELECT CASE WHEN count(*) FILTER (WHERE client_class IS DISTINCT FROM
                                  client_class_v0(cs_user_agent, cs_uri_stem, cs_method,
                                                  bot_category := bot_category, asn := cf_asn)
                                  OR rule_version <> 'v0') = 0
             AND count(*) = 12 THEN 'classified_access_v0 ok'
            ELSE error('classified_access_v0 differs from client_class_v0') END AS result
FROM classified_access_v0(DATE '2026-07-15', DATE '2026-07-16', src := 'client_class_v1_check_access');

-- End to end, with DB-IP and the crawler ranges: every row once, rule_version 'v1'.
WITH got AS (
    SELECT c_ip, cs_user_agent = getvariable('chrome') AS browser, client_class, rule_version
    FROM classified_access_v1(DATE '2026-07-15', DATE '2026-07-20', src := 'client_class_v1_check_access')
)
SELECT CASE WHEN count(*) = 16 AND bool_and(rule_version = 'v1')
             AND list_sort(list(c_ip || ' ' || browser || ' ' || client_class)
                           FILTER (WHERE c_ip IN ('45.78.192.10', '66.249.66.1', '23.24.1.1')))
                 = ['23.24.1.1 false package_client', '23.24.1.1 true human_browser',
                    '45.78.192.10 true likely_automated', '66.249.66.1 true search_crawler']
            THEN 'classified_access_v1 ok'
            ELSE error('classified_access_v1 mismatch: ' || string_agg(c_ip || ' → ' || client_class, '; ')) END AS result
FROM got;
