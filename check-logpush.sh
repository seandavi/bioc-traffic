#!/usr/bin/env bash
# ADR 0003: Logpush cannot backfill, so a silent delivery gap is permanent
# loss. Fail (-> bioc-notify@ -> GitHub issue) if a UTC day's prefix is
# missing any hour, or has fewer objects than a busy-enough floor.
#
#   ./check-logpush.sh            # yesterday (UTC); touches the dead-man stamp on success
#   ./check-logpush.sh 20260928   # any day; never touches the stamp
#
# Object presence is the check, not record counts: Logpush only writes an
# object when there are events, and the Worker logs one record per request.
# Object names are {start}_{end}_{hash}.log.gz with UTC timestamps; an object
# covers every hour its start..end span touches. A day's prefix also holds
# objects that started the previous day, so the span is clipped to the day.
# MIN_OBJECTS: post-cutover delivery is ~2600 objects/day (Sept 2026); the
# hour check catches partial-day outages the count can't.
set -euo pipefail

PREFIX=${LOGPUSH_PREFIX:-gs://bioc-u24-logs/cloudflare/bioc-access-logs}
MIN_OBJECTS=${MIN_OBJECTS:-1000}
STAMP=${STAMP:-${XDG_STATE_HOME:-$HOME/.local/state}/bioc-logpush-check.ok}
day=${1:-$(date -u -d yesterday +%Y%m%d)}   # Logpush {DATE} is UTC

case $PREFIX in
  gs://*) ls_cmd=(gcloud storage ls) ;;
  *:*)    ls_cmd=(rclone lsf) ;;   # e.g. r2:bucket/path
  *) echo "LOGPUSH_PREFIX must be gs://... or an rclone remote:path" >&2; exit 2 ;;
esac

# A listing error (or an empty prefix, which gcloud reports as an error)
# falls through as zero objects, which fails below with the error in the journal.
read -r n missing < <("${ls_cmd[@]}" "$PREFIX/$day/" | awk -v day="$day" '
  match($0, /[0-9]{8}T[0-9]{6}Z_[0-9]{8}T[0-9]{6}Z_[^\/]*\.log\.gz$/) {
    n++
    sd = substr($0, RSTART, 8);      sh = substr($0, RSTART + 9, 2) + 0
    ed = substr($0, RSTART + 17, 8); eh = substr($0, RSTART + 26, 2) + 0
    if (ed < day || sd > day) next
    for (h = (sd < day ? 0 : sh); h <= (ed > day ? 23 : eh); h++) seen[h] = 1
  }
  END {
    for (h = 0; h < 24; h++) if (!(h in seen)) miss = miss (miss ? "," : "") sprintf("%02d", h)
    print n + 0, (miss ? miss : "-")
  }' || true)

echo "logpush gap check: $PREFIX/$day/ -> $n objects (min $MIN_OBJECTS), missing UTC hours: $missing"
if [[ $missing != - ]] || (( n < MIN_OBJECTS )); then
  [[ $missing != - ]] && echo "GAP: no objects for $day UTC hours $missing." >&2
  (( n < MIN_OBJECTS )) && echo "GAP: $n objects delivered for $day (need >= $MIN_OBJECTS)." >&2
  echo "Logpush cannot backfill; every day this persists is unrecoverable." >&2
  echo "Check the Logpush job's health in the Cloudflare dashboard (job ID: ANALYTICS.local.md)." >&2
  exit 1
fi

# Dead-man stamp: bioc-logpush-check-stale.timer alerts if this goes stale.
if (( $# == 0 )); then
  mkdir -p "$(dirname "$STAMP")" && touch "$STAMP"
fi
