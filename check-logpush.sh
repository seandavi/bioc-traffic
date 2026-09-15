#!/usr/bin/env bash
# ADR 0003: Logpush cannot backfill, so a silent delivery gap is permanent
# loss. Fail (-> bioc-notify@ -> GitHub issue) if yesterday's UTC prefix in
# GCS has fewer delivered objects than expected.
#
# Object presence is the check, not record counts: Logpush only writes an
# object when there are events, and the Worker logs one record per request,
# so zero objects for a whole day means the push (or the site) is dead.
# ponytail: threshold check only; raise MIN_OBJECTS well above 24 after the
# cutover, when hourly delivery becomes guaranteed by traffic volume.
set -euo pipefail

PREFIX=${LOGPUSH_PREFIX:-gs://bioc-u24-logs/cloudflare/bioc-access-logs}
MIN_OBJECTS=${MIN_OBJECTS:-1}   # dev-era traffic delivers 7-24 objects/day
day=$(date -u -d yesterday +%Y%m%d)   # Logpush {DATE} is UTC

n=$(gcloud storage ls "$PREFIX/$day/" 2>/dev/null | grep -c '\.log\.gz$' || true)
echo "logpush gap check: $PREFIX/$day/ -> $n objects (min $MIN_OBJECTS)"
if (( n < MIN_OBJECTS )); then
  echo "GAP: $n objects delivered for $day (need >= $MIN_OBJECTS)." >&2
  echo "Logpush cannot backfill; every day this persists is unrecoverable." >&2
  echo "Check: job 1826554 status, and https://dash.cloudflare.com Logpush health." >&2
  exit 1
fi
