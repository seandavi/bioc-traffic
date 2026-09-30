#!/usr/bin/env bash
# Self-check for check-logpush.sh's hour coverage, against a fake prefix read
# through rclone's on-the-fly :local: backend (no bucket needed).
set -euo pipefail
tmp=$(mktemp -d "${TMPDIR:-/tmp}/check-logpush.XXXXXX"); trap 'rm -rf "$tmp"' EXIT
mkdir "$tmp/20260929"
touch "$tmp/20260929/"{20260928T220000Z_20260929T013000Z_a,20260929T030000Z_20260929T050500Z_b,20260929T230000Z_20260930T001000Z_c,20260928T100000Z_20260928T110000Z_d,20260930T010000Z_20260930T020000Z_e}.log.gz

out=$(LOGPUSH_PREFIX=":local:$tmp" MIN_OBJECTS=1 STAMP="$tmp/stamp" "$(dirname "$0")/check-logpush.sh" 20260929 2>&1) && { echo "FAIL: expected a gap"; exit 1; }
grep -q 'missing UTC hours: 02,06,07,08,09,10,11,12,13,14,15,16,17,18,19,20,21,22$' <<<"$out" || { echo "FAIL: $out"; exit 1; }
[[ ! -e $tmp/stamp ]] || { echo "FAIL: stamp touched for a dated run"; exit 1; }
echo ok
