-- client_class v0 — per-request traffic class, both eras (seandavi/bioc-traffic#5).
--
-- Load:   .read sql/client_class.sql        (DuckDB)
-- Check:  duckdb -c ".read sql/client_class.sql" -c ".read sql/client_class_check.sql"
--
-- CloudFront era (no ASN / bot label yet, #4):
--   client_class_v0(cs_user_agent, cs_uri_stem, cs_method)
-- Cloudflare era:
--   client_class_v0(cs_user_agent, cs_uri_stem, cs_method,
--                   cf->>'verifiedBotCategory', cf_asn)
-- cf_as_organization is not an input: see client_hosting_asn_v0 for why ASN is the key.
--
-- Deterministic: first matching rule wins, in the order below. Changing any rule or list means
-- a new version (client_class_v1, rule_version 'v1'), never an edit here, so history recomputes.
--
-- Classes: human_browser, search_crawler, ai_crawler, package_client, ci, mirror, monitoring,
-- likely_automated, unknown. is_human_v0() is the coarse split for #11: human_browser only.
--
-- sc_status is deliberately not an input: per request it separates nothing (bots and people get
-- the same 200s and 301s). It matters to the behavioural windows #5 lists, which v0 does not do.

CREATE OR REPLACE MACRO client_class_rule_version() AS 'v0';

-- CloudFront logs URL-encode the user agent (spaces as %20); Cloudflare's arrive raw. Undo only
-- the escapes the patterns below rely on: %20 (582k of 584k escapes in July 2026's distinct UAs)
-- and the braces of an unfilled '{version}'. Not url_decode: it rejects invalid UTF-8 (a
-- Baiduspider UA ends in %A3%A9), and try(url_decode(...)) segfaults DuckDB 1.5.5 over a full
-- month. A Cloudflare-era UA containing a literal '%20' would be rewritten too; none seen.
CREATE OR REPLACE MACRO client_ua_v0(ua) AS
    lower(replace(replace(replace(ua, '%20', ' '), '%7B', '{'), '%7D', '}'));

-- Datacenter, cloud and proxy-leasing ASNs seen sending browser user agents in the 2026-09-29
-- 24h sample, plus AS4229 from #5. ASN, not organisation: the org label changes per leased
-- block (AS9009 carries M247, code200, IPXO…), the ASN does not. Known ceiling: VPN egress
-- (M247, AS9009) sends real people through these too; #4's range lists are the upgrade.
CREATE OR REPLACE MACRO client_hosting_asn_v0(asn) AS asn IN (
    16509, 14618,            -- Amazon
    15169, 396982,           -- Google, Google Cloud
    8075,                    -- Microsoft / Azure
    31898,                   -- Oracle Cloud
    45102, 37963,            -- Alibaba Cloud
    132203, 45090,           -- Tencent Cloud
    55990, 136907,           -- Huawei Cloud
    150436, 4229,            -- BytePlus, Zerodesktop: the #5 disguised crawler
    14061, 63949, 20473,     -- DigitalOcean, Linode/Akamai, Vultr
    16276, 24940, 51167,     -- OVH, Hetzner, Contabo
    9009,                    -- M247
    212238, 396356, 59711,   -- Datacamp / code200 / Latitude / HZ Hosting
    27411, 30058, 35758, 47007, 264617,  -- code200, FDCservers, LeaseWeb blocks
    44144, 203020,           -- HostRoyale
    214483,                  -- RapidSeedbox
    50077, 7979,             -- SYN LTD, Servers.com
    62610, 398781,           -- IP-leasing blocks with ~1 request per address
    39855, 54103             -- MOD Mission Critical
);

-- u is client_ua_v0(ua): lower-cased, %20 and braces decoded.
CREATE OR REPLACE MACRO client_class_v0_(u, uri, method, bot_category, asn) AS CASE
    -- 1. Cloudflare's verifiedBotCategory is the only authoritative bot label, so it goes
    --    first. It is '' (not NULL) when unverified. 'Security', 'Accessibility' and 'Other'
    --    fall through: in the sample they are Zscaler / Netskope / iboss relaying real
    --    people's R sessions and browsers, not bots.
    WHEN bot_category IN ('AI Crawler', 'AI Search', 'AI Assistant') THEN 'ai_crawler'
    WHEN bot_category IN ('Search Engine Crawler', 'Search Engine Optimization',
                          'Advertising & Marketing', 'Archiver', 'Academic Research',
                          'Aggregator') THEN 'search_crawler'
    WHEN bot_category = 'Monitoring & Analytics' THEN 'monitoring'
    -- Verified fetchers acting for a person or a feed, with no class of their own.
    WHEN bot_category IN ('Page Preview', 'Feed Fetcher', 'Webhooks') THEN 'likely_automated'

    -- 2. No user agent at all: no browser or R session omits it.
    WHEN u IS NULL OR u IN ('', '-') THEN 'likely_automated'

    -- 3. Self-declared AI crawlers and assistants. Where Cloudflare's label disagrees with the
    --    bot's own branding, follow Cloudflare so the eras agree (PetalBot, Amazonbot: 'AI
    --    Crawler'; ShapBot: 'AI Search'). geminibot rides a 'libcurl/x Geminibot' UA, so this
    --    must precede the package_client libcurl rule.
    WHEN regexp_matches(u, 'gptbot|chatgpt-user|oai-searchbot|claudebot|claude-user|'
                         || 'claude-searchbot|anthropic-ai|perplexity|ccbot|bytespider|'
                         || 'amazonbot|meta-externalagent|meta-externalfetcher|'
                         || 'google-extended|googleother|geminibot|applebot-extended|'
                         || 'cohere-ai|diffbot|youbot|shapbot|reflectionbot|timpibot|ai2bot|'
                         || 'petalbot|duckassistbot|mistralai') THEN 'ai_crawler'

    -- 4. Self-declared search and SEO crawlers.
    WHEN regexp_matches(u, 'googlebot|adsbot-google|mediapartners-google|bingbot|yisouspider|'
                         || 'baiduspider|yandex|duckduckbot|sogou|360spider|haosouspider|'
                         || 'applebot|seznambot|qwantbot|coccocbot|yeti/|mojeekbot|'
                         || 'meta-webindexer|semrushbot|ahrefsbot|mj12bot|dotbot|'
                         || 'dataforseobot|blexbot|serpstatbot|barkrowler|archive\.org_bot|'
                         || 'ia_archiver') THEN 'search_crawler'

    -- 5. Uptime, link and release checkers. nvchecker polls for new package versions for
    --    distro packaging; the bio.tools linter walks registry links.
    WHEN regexp_matches(u, 'uptimerobot|pingdom|statuscake|site24x7|betteruptime|uptime-kuma|'
                         || 'datadog|newrelic|nagios|check_http|zabbix|checkly|hosttracker|'
                         || 'freshping|updown\.io|blackbox-exporter|nvchecker|biotools-linter|'
                         || 'linkcheck|lychee|w3c-checklink') THEN 'monitoring'

    -- 6. CI and package builders, by explicit marker only. R on GitHub Actions looks like any
    --    other R session until ASN is available for both eras (#4), so it lands in
    --    package_client.
    WHEN regexp_matches(u, 'bioconda-utils|github-actions|gitlab-runner|travis|circleci|'
                         || 'jenkins|buildkite|azure-pipelines|teamcity|r-universe|easybuild')
        THEN 'ci'

    -- 7. Bulk copiers and repository proxies. v0 cannot tell `wget -m` from a one-off wget,
    --    and wget is the classic mirroring tool, so all of it goes here.
    WHEN regexp_matches(u, '^rclone/|^wget/|rsync|lftp|httrack|aria2|offline explorer|'
                         || 'sitesucker|webcopier|teleport pro|cloudsmith|artifactory|nexus/[0-9]')
        THEN 'mirror'

    -- 8. R and the R ecosystem: R's own "R (4.6.1 platform ...)" (install.packages,
    --    BiocManager), RStudio / renv / Posit Cloud wrappers, rocker / setup-r style "R/4.x ...",
    --    r-curl / httr clients (AnnotationHub, BiocFileCache), and bare libcurl/x.y.z, which
    --    is what R's libcurl method sends and which mostly fetches /packages/ indexes and
    --    tarballs.
    WHEN regexp_matches(u, '(^|[ ;])r \([0-9]|(^| )r/[0-9]|rstudio|^renv |biocmanager|r-curl/|'
                         || 'httr2?/|^libcurl/[0-9.]+( google)?$') THEN 'package_client'

    -- 9. Scripted clients, headless browsers, link-preview and feed fetchers (Cloudflare's
    --    'Page Preview' / 'Feed Fetcher', for the CloudFront era), other self-declared bots.
    WHEN regexp_matches(u, 'python-requests|python-urllib|python-httpx|aiohttp|^python/|'
                         || 'go-http-client|^curl/|^java/|okhttp|apache-httpclient|axios|'
                         || 'node-fetch|undici|^node|^ruby|libwww-perl|^perl|guzzlehttp|'
                         || 'scrapy|headlesschrome|phantomjs|puppeteer|playwright|selenium|'
                         || 'lightpanda|facebookexternalhit|github-camo|feedburner|feedly|nextcloud-news|'
                         || 'bot|crawl|spider|scrape') THEN 'likely_automated'

    -- 10. Browser-shaped UAs no real browser sends: ',gzip(gfe)' is appended by Google's front
    --     end to requests fetched from Google Cloud (in the sample ~1M requests over 656k
    --     distinct /checkResults/ paths from AS15169, eight UAs at ~123k each); an unfilled
    --     '{version}' template; Safari tokens like 'Safari/184.1'; the Nexus 5 / Chrome 65
    --     string headless tools emulate by default; and 'Mozilla/5.0' with no platform.
    WHEN regexp_matches(u, 'gzip\(gfe\)|\{version\}|safari/[12][0-9]{2}\.[0-9]$|'
                         || 'nexus 5 build/mra58n|^mozilla/[0-9.]+( gecko/[0-9]+ firefox/[0-9.]+)?$')
        THEN 'likely_automated'

    -- 11. Paths only automation requests. The #5 crawler follows relative links recursively,
    --     producing /talks/2014/SeattleOct2014/2014/SeattleOct2014/...: two or more year
    --     segments under /talks/. UA-independent, so it catches the crawler in the CloudFront
    --     era, where it is ~24% of July 2026. Plus vulnerability-scanner probes.
    WHEN regexp_matches(uri, '^/+talks/+([^/]+/+)*20[0-9]{2}/+([^/]+/+)*20[0-9]{2}/')
      OR regexp_matches(lower(uri), 'wp-login\.php|xmlrpc\.php|/wp-admin|/\.env|/\.git/|'
                                  || 'phpmyadmin') THEN 'likely_automated'

    -- 12. Browsers. A browser UA from a datacenter ASN is the #5 disguised crawler's shape
    --     (Mac Chrome 131/133/136 from BytePlus); browsers don't navigate with HEAD.
    WHEN regexp_matches(u, '^(mozilla|opera)/')
     AND regexp_matches(u, 'chrome/|firefox/|safari/|edg/|opr/|msie |trident/|crios/|fxios/')
        THEN CASE WHEN client_hosting_asn_v0(asn) THEN 'likely_automated'
                  WHEN method = 'HEAD' THEN 'likely_automated'
                  ELSE 'human_browser' END

    ELSE 'unknown'
END;

CREATE OR REPLACE MACRO client_class_v0(ua, uri, method,
                                        bot_category := NULL, asn := NULL) AS
    client_class_v0_(client_ua_v0(ua), uri, method, bot_category, asn);

CREATE OR REPLACE MACRO is_human_v0(client_class) AS client_class = 'human_browser';
