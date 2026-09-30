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
# written aside and renamed so a reader never sees a partial file. `day` also runs rollup-overall.
# Dashboard rollups (#10): just rollup minute|hour|day
rollup tier:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p {{rollups}}
    out={{rollups}}/{{tier}}
    just duckdb -c "{{rollup_limits}}" -c ".read sql/rollup_tier.sql" -c "
      SET VARIABLE t1 = now() AT TIME ZONE 'UTC';
      SET VARIABLE t0 = date_trunc('{{tier}}', getvariable('t1') - rollup_window('{{tier}}'));
      COPY (FROM rollup_tier('{{tier}}', getvariable('t0'), getvariable('t1')))
        TO '$out.parquet.tmp' (FORMAT parquet, COMPRESSION zstd);
      COPY (SELECT '{{tier}}' AS grain, getvariable('t0') AS window_start,
                   getvariable('t1') AS generated_at, client_class_rule_version() AS rule_version,
                   (SELECT list(r) FROM (SELECT * EXCLUDE (rule_version)
                                         FROM read_parquet('$out.parquet.tmp')
                                         WHERE dimension = 'overall' OR t IS NULL) r) AS rows)
        TO '$out.json.tmp' (FORMAT json);"
    mv "$out.parquet.tmp" "$out.parquet"
    mv "$out.json.tmp" "$out.json"
    if [ {{tier}} = day ]; then just rollup-overall; fi

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
