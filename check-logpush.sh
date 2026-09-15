#!/usr/bin/env bash
# ADR 0003: Logpush cannot backfill, so a silent delivery gap is permanent
# loss. Fail (-> bioc-notify@ -> GitHub issue) if yesterday's UTC prefix in
# GCS has fewer delivered objects than expected.
#
# Object presence is the check, not record counts: Logpush only writes an
# object when there are events, and the Worker logs one record per request,
# so zero objects for a whole day means the push (or the site) is dead.
# ponytail: threshold check only. Post-cutover delivery is ~2600 objects/day
# (Sept 2026); 1000 trips on a gap of roughly nine hours without false alarms
# on a slow day.
set -euo pipefail

PREFIX=${LOGPUSH_PREFIX:-gs://bioc-u24-logs/cloudflare/bioc-access-logs}
MIN_OBJECTS=${MIN_OBJECTS:-1000}   # ~2600/day since the cutover
day=$(date -u -d yesterday +%Y%m%d)   # Logpush {DATE} is UTC

n=$(gcloud storage ls "$PREFIX/$day/" 2>/dev/null | grep -c '\.log\.gz$' || true)
echo "logpush gap check: $PREFIX/$day/ -> $n objects (min $MIN_OBJECTS)"
if (( n < MIN_OBJECTS )); then
  echo "GAP: $n objects delivered for $day (need >= $MIN_OBJECTS)." >&2
  echo "Logpush cannot backfill; every day this persists is unrecoverable." >&2
  echo "Check: job 1826554 status, and https://dash.cloudflare.com Logpush health." >&2
  exit 1
fi
