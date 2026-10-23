# Cloudflare-era Parquet (#8) and the unified access view. `just --list` for recipes.

script := "./cloudflare-logs-to-parquet.py"

# Run the built-in checks (client_id test vector, projection)
self-check:
    {{script}} --self-check

# Build UTC days or hours locally, verified, no upload: just build 2026-09-29 [2026-09-30]
build from to=from:
    {{script}} --from {{from}} --to {{to}}

# Reconcile access-record counts, raw objects vs Parquet
verify from to=from:
    {{script}} --verify --from {{from}} --to {{to}}

# Build, verify, then upload to R2
publish from to=from:
    {{script}} --from {{from}} --to {{to}} --upload

# What the 15-minute timer runs: current and previous UTC hour
recent:
    {{script}} --recent --upload

# What the nightly timer runs: re-seal yesterday (UTC)
reseal:
    {{script}} --yesterday --upload

# Everything from the first delivered day through the given day
backfill to:
    {{script}} --from 2026-08-06 --to {{to}} --upload

# DuckDB with sql/{access,client_class,downloads,ipref}.sql loaded, salt set: just duckdb [-c "SELECT ..."]
# The salt goes through the environment, not argv or disk; $(...) trims its newline.
[positional-arguments]
duckdb *args:
    @BIOC_IP_SALT="$(gcloud secrets versions access latest --secret bioc-logs-ip-salt --project cdsci-infra)" \
      duckdb -init sql/access.sql -cmd "SET VARIABLE ip_salt = getenv('BIOC_IP_SALT')" \
        -cmd ".read sql/client_class.sql" -cmd ".read sql/downloads.sql" -cmd ".read sql/ipref.sql" "$@"

# Package download stats (#11): stale monthly partitions, aggregates (with the published
# months before 2020 from published_month.parquet, #24), the /packages/stats/ tree
stats *args:
    ./download-stats.py {{args}}

# download-stats.py checks: downloads view = DOWNLOADS_SQL, aggregates, .tab format
stats-self-check:
    ./download-stats.py --self-check

stats_tree := env("BIOC_STATS_TREE", "/data/davsean/bioc-traffic-stats/stats")
stats_dest := env("BIOC_STATS_DEST", "r2:bioc-site/packages/stats")

# The Worker's bucket, where bioc-edge serves /packages/stats/*.tab from (bioc-infrastructure#114).
# The tree is rewritten every run, so --checksum (MD5 against R2's ETag, from the listing) is
# what keeps this to the files whose bytes changed. copy, not sync: nothing is deleted.
# text/plain as master serves them. Pass --dry-run to see what would move.
# The pages (#28) and static/barchart.js go the same way, one rclone per type so each gets its
# own Content-Type (--header-upload applies to every file of a run).
# The /packages/stats/ .tab files, HTML pages and barchart.js to R2, changed files only: just stats-upload [--dry-run]
stats-upload *args:
    rclone copy {{stats_tree}} {{stats_dest}} --include '*.tab' --checksum --transfers 16 \
      --header-upload 'Content-Type: text/plain; charset=utf-8' --stats-one-line -v {{args}}
    rclone copy {{stats_tree}} {{stats_dest}} --include '*.html' --checksum --transfers 16 \
      --header-upload 'Content-Type: text/html; charset=utf-8' --stats-one-line -v {{args}}
    rclone copy {{stats_tree}} {{stats_dest}} --include 'static/*.js' --checksum --transfers 16 \
      --header-upload 'Content-Type: text/javascript' --stats-one-line -v {{args}}

rollups := env("BIOC_ROLLUPS", "/data/davsean/bioc-traffic-rollups")
# A shared, busy host: cap DuckDB, spill to /data rather than /tmp.
rollup_limits := "SET memory_limit = '32GB'; SET threads = 16; SET temp_directory = '/data/davsean/tmp/duckdb-rollups';"

# <tier>.parquet is the whole tier, <tier>.json its overall series and window totals, both
# written aside and renamed so a reader never sees a partial file. minute recomputes its 6 h;
# hour and day recompute only what upstream can have changed (rollup_late in
# sql/rollup_tier.sql, or from yesterday with --since-yesterday, which the daily run uses after
# the re-seal) and keep the rest. --full rebuilds the tier: after a classifier version bump or
# a fix. `day` also runs rollup-overall and rollup-clients.
# Dashboard rollups (#10, #19): just rollup minute|hour|day [--full|--since-yesterday]
rollup tier mode="":
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{rollups}}
    out={{rollups}}/{{tier}}
    case "{{tier}} {{mode}}" in
      "minute ")
        rows="FROM rollup_tier('minute', getvariable('t0'), getvariable('t1'))"
        ti="NULL" ;;
      "hour --full" | "day --full")
        rows="FROM rollup_tier('{{tier}}', getvariable('t0'), getvariable('t1'), per_bucket := true)"
        ti="NULL" ;;
      "hour " | "day " | "hour --since-yesterday" | "day --since-yesterday")
        [ -f "$out.parquet" ] || { echo "no $out.parquet: run with --full first" >&2; exit 1; }
        rows="FROM rollup_merge('{{tier}}', '$out.parquet', getvariable('t0'), getvariable('ti'),
                                getvariable('t1'))"
        since="NULL"
        [ -z "{{mode}}" ] || since="date_trunc('day', getvariable('t1')) - INTERVAL 1 DAY"
        ti="rollup_start('{{tier}}', getvariable('t0'), getvariable('t1'),
                         (SELECT max(t) FROM read_parquet('$out.parquet')), since := $since)" ;;
      *) echo "usage: just rollup minute | just rollup hour|day [--full|--since-yesterday]" >&2
         exit 2 ;;
    esac
    if [ {{tier}} = minute ]; then
      window="FROM read_parquet('$out.parquet.tmp') WHERE t IS NULL"
      note="NULL"
    else
      window="SELECT NULL::TIMESTAMP AS t, dimension, value, client_class, NULL AS rule_version,
                     CAST(sum(requests) AS BIGINT) AS requests, CAST(sum(bytes) AS BIGINT) AS bytes,
                     NULL::BIGINT AS clients
              FROM read_parquet('$out.parquet.tmp') GROUP BY dimension, value, client_class"
      note="'Window rows (t null) sum the per-t rows. Top-N is per t, so page, referrer, '
            || 'ua_family and package window values are approximate: a value never in the top N '
            || 'of any single t is in (other). clients is null there on purpose: distinct '
            || 'clients do not add. Exact window clients: window_clients.json.'"
    fi
    just duckdb -c "{{rollup_limits}}" -c ".read sql/rollup_tier.sql" -c "
      SET VARIABLE t1 = now() AT TIME ZONE 'UTC';
      SET VARIABLE t0 = date_trunc('{{tier}}', getvariable('t1') - rollup_window('{{tier}}'));
      SET VARIABLE ti = $ti;
      COPY ($rows) TO '$out.parquet.tmp' (FORMAT parquet, COMPRESSION zstd);
      COPY (SELECT '{{tier}}' AS grain, getvariable('t0') AS window_start,
                   getvariable('t1') AS generated_at, client_class_rule_version() AS rule_version,
                   $note AS note,
                   (SELECT list(r) FROM (SELECT * EXCLUDE (rule_version)
                                         FROM (FROM read_parquet('$out.parquet.tmp')
                                               WHERE dimension = 'overall' AND t IS NOT NULL
                                               UNION ALL $window)) r) AS rows)
        TO '$out.json.tmp' (FORMAT json);"
    mv "$out.parquet.tmp" "$out.parquet"
    mv "$out.json.tmp" "$out.json"
    if [ {{tier}} = day ]; then just rollup-overall; just rollup-clients; fi

# window_clients.parquet + .json, since the hour and day tiers' window rows have no clients.
# One 90 d scan (per_client as in rollup_tier); `just rollup day` runs it.
# Exact distinct clients over the 30 d and 90 d tier windows, overall and per client_class
rollup-clients:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{rollups}}
    out={{rollups}}/window_clients
    just duckdb -c "{{rollup_limits}}" -c ".read sql/rollup_tier.sql" -c "
      SET VARIABLE t1 = now() AT TIME ZONE 'UTC';
      COPY (FROM rollup_window_clients(getvariable('t1')))
        TO '$out.parquet.tmp' (FORMAT parquet, COMPRESSION zstd);
      COPY (SELECT getvariable('t1') AS generated_at,
                   (SELECT list(r) FROM read_parquet('$out.parquet.tmp') r) AS rows)
        TO '$out.json.tmp' (FORMAT json);"
    mv "$out.parquet.tmp" "$out.parquet"
    mv "$out.json.tmp" "$out.json"

# The first run computes it from 2020; later runs recompute the last 3 UTC days (late records,
# the 08:00 re-seal) and keep the rest.
# The forever daily series, both eras, overall only
rollup-overall:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{rollups}}
    out={{rollups}}/overall_day
    today="CAST(now() AT TIME ZONE 'UTC' AS DATE)"
    if [ -f "$out.parquet" ]; then
      d0="$today - 3"; keep="FROM read_parquet('$out.parquet') WHERE day < $d0 UNION ALL"
    else
      d0="DATE '2020-01-01'"; keep=""
    fi
    just duckdb -c "{{rollup_limits}}" -c ".read sql/rollup_overall.sql" -c "
      COPY ($keep FROM rollup_overall_day($d0, $today) ORDER BY day, era)
        TO '$out.parquet.tmp' (FORMAT parquet, COMPRESSION zstd);
      COPY (SELECT now() AT TIME ZONE 'UTC' AS generated_at,
                   (SELECT list(r) FROM read_parquet('$out.parquet.tmp') r) AS rows)
        TO '$out.json.tmp' (FORMAT json);"
    mv "$out.parquet.tmp" "$out.parquet"
    mv "$out.json.tmp" "$out.json"

# Copy the rollups to R2 (the private bucket; nothing here is public)
rollup-upload:
    rclone copy {{rollups}} r2:bioc-access-logs/rollups/ --exclude '*.tmp'

dashboard := env("BIOC_DASHBOARD", "/data/davsean/bioc-traffic-dashboard")
dashboard_dest := env("BIOC_DASHBOARD_DEST", "r2:bioc-site/packages/stats")
dashboard_url := env("BIOC_DASHBOARD_URL", "https://bioconductor.org/packages/stats/static/traffic.json")

# traffic_day.parquet (sql/traffic.sql) is the forever series, complete UTC days through
# yesterday. The first run, --full, or a client_class rule version other than the stored one
# backfills it from 2020 a month at a time (~45 min); later runs recompute from the last stored
# day or 3 days back, whichever is earlier, so missed runs leave no hole. Then
# static/traffic.json (the series, era-dense, null on days an era has no data; top pages over
# the last 30 days) and traffic/index.html, laid out as under /packages/stats/, each written
# aside and renamed. Preview: python3 -m http.server -d {{dashboard}}, then /traffic/.
# The public traffic dashboard (#6): just traffic [--full]
traffic mode="":
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{dashboard}}/static {{dashboard}}/traffic
    series={{dashboard}}/traffic_day.parquet
    today=$(date -u +%F)
    d1=$(date -u -d "$today -1 day" +%F)
    load=(-c "{{rollup_limits}}" -c ".read sql/rollup_tier.sql" -c ".read sql/traffic.sql")
    current=$(just duckdb -noheader -list -c "SELECT client_class_rule_version()")
    stored=""
    [ ! -f "$series" ] || stored=$(duckdb -noheader -list -c "SELECT string_agg(DISTINCT rule_version, ',') FROM '$series'")
    if [ -z "{{mode}}" ] && [ "$stored" = "$current" ]; then
      just duckdb "${load[@]}" -c "
        SET VARIABLE d1 = DATE '$d1';
        SET VARIABLE d0 = least(DATE '$today' - 3, (SELECT max(day) FROM read_parquet('$series')));
        COPY (FROM read_parquet('$series') WHERE day < getvariable('d0')
              UNION ALL FROM traffic_day(getvariable('d0'), getvariable('d1'))
              ORDER BY day, era, surface, client_class NULLS FIRST)
          TO '$series.tmp' (FORMAT parquet, COMPRESSION zstd);"
    elif [ -z "{{mode}}" ] || [ "{{mode}}" = --full ]; then
      echo "backfilling $series from 2020 (stored rule version: ${stored:-none}, current: $current)" >&2
      parts=$series.parts
      rm -rf "$parts"; mkdir -p "$parts"
      sql="" m=2020-01-01
      while [[ ! "$m" > "$d1" ]]; do
        e=$(date -u -d "$m +1 month -1 day" +%F); [[ "$e" > "$d1" ]] && e=$d1
        sql+="COPY (FROM traffic_day(DATE '$m', DATE '$e')) TO '$parts/$m.parquet' (FORMAT parquet);"
        m=$(date -u -d "$m +1 month" +%F)
      done
      just duckdb "${load[@]}" -c "$sql" -c "
        COPY (FROM read_parquet('$parts/*.parquet') ORDER BY day, era, surface, client_class NULLS FIRST)
          TO '$series.tmp' (FORMAT parquet, COMPRESSION zstd);"
      rm -rf "$parts"
    else
      echo "usage: just traffic [--full]" >&2; exit 2
    fi
    mv "$series.tmp" "$series"
    json={{dashboard}}/static/traffic.json
    just duckdb "${load[@]}" -c "
      SET VARIABLE d1 = DATE '$d1';
      CREATE TEMP TABLE s AS FROM read_parquet('$series') WHERE day <= getvariable('d1');
      CREATE TEMP TABLE top AS FROM traffic_top_pages(getvariable('d1') - 29, getvariable('d1'));
      COPY (
        WITH eras AS (SELECT era, min(day) AS d0, max(day) AS d1 FROM s GROUP BY era),
        present AS (SELECT DISTINCT era, day FROM s),
        grid AS (
          SELECT k.era, k.surface, k.client_class, e.d0, g.day, p.day IS NOT NULL AS present
          FROM (SELECT DISTINCT era, surface, client_class FROM s) k
          JOIN eras e USING (era)
          CROSS JOIN LATERAL (SELECT unnest(range(e.d0, e.d1 + 1, INTERVAL 1 DAY))::DATE AS day) g
          LEFT JOIN present p ON p.era = k.era AND p.day = g.day),
        series AS (
          SELECT g.era, g.surface, g.client_class, g.d0 AS start,
                 list(CASE WHEN present THEN coalesce(s.requests, 0) END ORDER BY g.day) AS requests,
                 list(CASE WHEN present THEN coalesce(s.clients, 0) END ORDER BY g.day) AS clients
          FROM grid g LEFT JOIN s ON s.era = g.era AND s.day = g.day AND s.surface = g.surface
                                 AND s.client_class IS NOT DISTINCT FROM g.client_class
          GROUP BY ALL ORDER BY g.surface DESC, g.client_class NULLS FIRST, start)
        SELECT strftime(now() AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS generated_at,
               48 AS stale_after_hours, getvariable('d1') AS data_through,
               client_class_rule_version() AS rule_version,
               (SELECT min(day) FROM s WHERE era = 'cloudflare') AS era_boundary,
               'Daily UTC requests and distinct clients (salted address hashes) by client_class, '
               || 'production hosts only. surface package = package downloads on the '
               || '/packages/stats/ definition, web = everything else. client_class null = all '
               || 'classes. One series per era: from era_boundary the Cloudflare era adds ASN and '
               || 'bot labels to the rules, so classes step there; arrays run from start, a day '
               || 'per element, null where that era has no data. Distinct clients do not add '
               || 'across days, eras or classes. top_pages: human_browser 200s for document '
               || 'paths, pages with at least 10 distinct clients.' AS note,
               (SELECT {start: getvariable('d1') - 29, \"end\": getvariable('d1'),
                        rows: coalesce(list(top ORDER BY requests DESC, page), [])} FROM top) AS top_pages,
               (SELECT list(series) FROM series) AS series)
        TO '$json.tmp' (FORMAT json);"
    mv "$json.tmp" "$json"
    cp static/traffic.html {{dashboard}}/traffic/index.html.tmp
    mv {{dashboard}}/traffic/index.html.tmp {{dashboard}}/traffic/index.html
    echo "$json: $(stat -c %s "$json") bytes, through $d1, rule version $current" >&2

# The Worker answers /packages/stats/<dir>/ and /packages/stats/static/* from R2 (bioc-edge
# statsKey), so the page and its JSON need no Worker change. traffic_day.parquet stays here.
# The dashboard page and JSON to the site bucket: just traffic-upload [--dry-run]
traffic-upload *args:
    rclone copy {{dashboard}} {{dashboard_dest}} --include 'static/traffic.json' --checksum \
      --header-upload 'Content-Type: application/json' --stats-one-line -v {{args}}
    rclone copy {{dashboard}} {{dashboard_dest}} --include 'traffic/index.html' --checksum \
      --header-upload 'Content-Type: text/html; charset=utf-8' --stats-one-line -v {{args}}

# The alarm ADR 0004 asks for: fails (non-zero exit) if traffic.json is missing, or its
# generated_at is older than its own stale_after_hours. A URL or a local path.
# Is the published dashboard data stale? just traffic-check-stale [url-or-path]
traffic-check-stale src=dashboard_url:
    duckdb -c "SELECT CASE WHEN now() - CAST(generated_at AS TIMESTAMPTZ) > to_hours(stale_after_hours) \
                      THEN error('traffic.json is stale: generated_at ' || generated_at) \
                      ELSE generated_at END AS generated_at FROM read_json('{{src}}')"

published := env("BIOC_PUBLISHED", "/data/davsean/bioc-published-stats")

# The published /packages/stats/ files as a source (#24). A one-time snapshot: to re-snapshot,
# run published-crawl (today's date, a new raw/<date>/), published-upload <date>,
# published-normalise, then `just stats` picks the history up.
# Land /packages/stats/ and /packages/oldstats/ raw, gzipped, resumable: just published-crawl [--date D]
published-crawl *args:
    ./published-stats-mirror.py crawl {{args}}

# Copy a raw snapshot to R2 and check it: just published-upload 2026-10-01
published-upload date:
    rclone copy {{published}}/raw/{{date}}/ r2:bioc-access-logs/published-stats/raw/{{date}}/ --exclude '*.partial'
    rclone check {{published}}/raw/{{date}}/ r2:bioc-access-logs/published-stats/raw/{{date}}/ --exclude '*.partial'

# published_month / published_year Parquet from the latest (or --date) raw snapshot
published-normalise *args:
    ./published-stats-mirror.py normalise {{args}}

# Published vs our fixed columns, 2020-01 → 2026-09, to docs/published-comparison.md
published-compare:
    ./published-stats-mirror.py compare > docs/published-comparison.md.partial
    mv docs/published-comparison.md.partial docs/published-comparison.md

# published-stats-mirror.py checks: link discovery, path canonicalisation
published-self-check:
    ./published-stats-mirror.py --self-check

# ASN and country by address, both eras (#4): DB-IP Lite's release (default this month's, kept
# beside the earlier ones) and the Google, Bing, OpenAI, Anthropic crawler ranges, to
# /data/davsean/bioc-traffic-ipref. Queried through sql/ipref.sql.
# Land a DB-IP Lite release and today's crawler ranges: just ipref [--release YYYY-MM]
ipref *args:
    ./ipref.py {{args}}

# ipref.py checks: ip_num = Python's ipaddress, a merged release = a brute-force lookup
ipref-self-check:
    ./ipref.py --self-check

# No salt needed: it reads cloudflare_access, not client_id.
# DB-IP vs cf_asn / cf_country, one Cloudflare-era UTC day: just ipref-agreement 2026-10-22
ipref-agreement day:
    duckdb -init sql/access.sql -cmd ".read sql/ipref.sql" -c "SET VARIABLE day = DATE '{{day}}'" \
      -c ".read sql/ipref_agreement.sql" < /dev/null
