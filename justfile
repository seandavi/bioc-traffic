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

# DuckDB with sql/access.sql loaded and the salt set: just duckdb [-c "SELECT ..."]
# The salt goes through the environment, not argv or disk; $(...) trims its newline.
[positional-arguments]
duckdb *args:
    @BIOC_IP_SALT="$(gcloud secrets versions access latest --secret bioc-logs-ip-salt --project cdsci-infra)" \
      duckdb -init sql/access.sql -cmd "SET VARIABLE ip_salt = getenv('BIOC_IP_SALT')" "$@"
