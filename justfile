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

# DuckDB with sql/{access,client_class,downloads}.sql loaded, salt set: just duckdb [-c "SELECT ..."]
# The salt goes through the environment, not argv or disk; $(...) trims its newline.
[positional-arguments]
duckdb *args:
    @BIOC_IP_SALT="$(gcloud secrets versions access latest --secret bioc-logs-ip-salt --project cdsci-infra)" \
      duckdb -init sql/access.sql -cmd "SET VARIABLE ip_salt = getenv('BIOC_IP_SALT')" \
        -cmd ".read sql/client_class.sql" -cmd ".read sql/downloads.sql" "$@"

# Package download stats (#11): stale monthly partitions, aggregates, the /packages/stats/ tree
stats *args:
    ./download-stats.py {{args}}

# download-stats.py checks: downloads view = DOWNLOADS_SQL, aggregates, .tab format
stats-self-check:
    ./download-stats.py --self-check

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
        # Back to the last stored t too, in case runs were missed.
        ti="greatest(getvariable('t0'), least((SELECT max(t) FROM read_parquet('$out.parquet')),"
        if [ -z "{{mode}}" ]; then
          ti="$ti date_trunc('{{tier}}', getvariable('t1') - rollup_late('{{tier}}'))))"
        else
          ti="$ti date_trunc('day', getvariable('t1')) - INTERVAL 1 DAY))"
        fi ;;
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
