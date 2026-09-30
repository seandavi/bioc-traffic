-- Known UA → class cases for client_class_v0. Fails (non-zero exit) on the first mismatch.
--   duckdb -c ".read sql/client_class.sql" -c ".read sql/client_class_check.sql"
WITH cases(ua, uri, method, bot_category, asn, expected) AS (VALUES
    -- CloudFront era: URL-encoded UA, no Cloudflare inputs
    ('R%20(4.6.1%20x86_64-w64-mingw32%20x86_64%20mingw32)', '/packages/3.22/bioc/src/contrib/limma_3.66.0.tar.gz', 'GET', NULL, NULL, 'package_client'),
    ('RStudio Desktop (2026.9.0.174); R (4.6.1 x86_64-w64-mingw32 x86_64 mingw32)', '/packages/3.22/bioc/bin/windows/contrib/4.6/limma_3.66.0.zip', 'GET', NULL, NULL, 'package_client'),
    ('posit.cloud R (4.6.1 x86_64-pc-linux-gnu x86_64 linux-gnu)', '/packages/3.22/bioc/src/contrib/PACKAGES', 'GET', NULL, NULL, 'package_client'),
    ('facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)', '/', 'GET', NULL, NULL, 'likely_automated'),
    ('libcurl/7.68.0', '/packages/3.19/bioc/src/contrib/PACKAGES', 'GET', NULL, NULL, 'package_client'),
    ('httr2/1.3.0 r-curl/8.0.0 libcurl/8.5.0', '/packages/json/3.22/tree.json', 'GET', NULL, NULL, 'package_client'),
    ('libcurl/8.5.0 Geminibot', '/', 'GET', NULL, NULL, 'ai_crawler'),
    ('rclone/', '/packages/3.23/bioc/src/contrib/', 'HEAD', NULL, NULL, 'mirror'),
    ('Wget/1.25.0', '/packages/3.14/bioc/src/contrib/a4_1.42.0.tar.gz', 'GET', NULL, NULL, 'mirror'),
    ('cloudsmith/1.1409.2 (+https://docs.cloudsmith.com/proxy-agent)', '/packages/3.22/bioc/src/contrib/limma_3.66.0.tar.gz', 'GET', NULL, NULL, 'mirror'),
    ('bioconda/bioconda-utils', '/packages/3.22/bioc/src/contrib/limma_3.66.0.tar.gz', 'GET', NULL, NULL, 'ci'),
    ('YisouSpider', '/style/components/x.css', 'GET', NULL, NULL, 'search_crawler'),
    ('Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko); compatible; ShapBot/0.1.0', '/', 'GET', NULL, NULL, 'ai_crawler'),
    ('lilydjwg/nvchecker 2.21', '/packages/release/bioc/html/limma.html', 'GET', NULL, NULL, 'monitoring'),
    ('python-requests/2.34.2', '/', 'HEAD', NULL, NULL, 'likely_automated'),
    (NULL, '/', 'GET', NULL, NULL, 'likely_automated'),
    ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36,gzip(gfe)', '/checkResults/3.24/bioc-LATEST/', 'GET', NULL, NULL, 'likely_automated'),
    ('Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010.15;%20rv:%7Bversion%7D.0)%20Gecko/20100101%20Firefox/135.0', '/', 'GET', NULL, NULL, 'likely_automated'),
    ('Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/131.0.0.0%20Safari/537.36', '/talks/2014/SeattleOct2014/2014/SeattleOct2014/B02.5_GeneSetEnrichment.Rmd', 'GET', NULL, NULL, 'likely_automated'),
    ('Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/131.0.0.0%20Safari/537.36', '/packages/release/bioc/html/limma.html', 'GET', NULL, NULL, 'human_browser'),
    ('(compatible;%20Baiduspider/2.0;%20+http://www.baidu.com/search/spider.html%A3%A9', '/', 'GET', NULL, NULL, 'search_crawler'),
    -- Cloudflare era: the #5 disguised crawler, a verified bot, and a Zscaler-relayed R session
    ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36', '/help/course-materials/', 'GET', '', 150436, 'likely_automated'),
    ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36', '/help/course-materials/', 'GET', '', 7922, 'human_browser'),
    ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36', '/', 'GET', 'AI Crawler', 32934, 'ai_crawler'),
    ('R (4.6.1 x86_64-pc-linux-gnu x86_64 linux-gnu)', '/packages/3.22/bioc/src/contrib/PACKAGES', 'GET', 'Security', 22616, 'package_client'),
    ('R/4.4.0 R (4.4.0 x86_64-pc-linux-gnu x86_64 linux-gnu)', '/packages/3.20/bioc/src/contrib/PACKAGES', 'GET', '', 14618, 'package_client'),
    ('msh-pdfmin/1', '/', 'GET', '', 45102, 'unknown')
),
got AS (
    SELECT *, client_class_v0(ua, uri, method, bot_category := bot_category, asn := asn) AS actual
    FROM cases
)
SELECT CASE WHEN count(*) FILTER (WHERE actual IS DISTINCT FROM expected) = 0
            THEN 'client_class_v0: ' || count(*) || ' cases ok'
            ELSE error('client_class_v0 mismatch: ' || string_agg(coalesce(ua, 'NULL') || ' → '
                       || actual || ' (expected ' || expected || ')', '; ')
                       FILTER (WHERE actual IS DISTINCT FROM expected)) END AS result
FROM got;

SELECT CASE WHEN client_class_rule_version() = 'v0' AND is_human_v0('human_browser')
             AND NOT is_human_v0('package_client') THEN 'rule_version / is_human ok'
            ELSE error('rule_version / is_human_v0 broken') END AS result;
