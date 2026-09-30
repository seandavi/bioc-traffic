#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb>=1.4"]
# ///
"""Turn the Cloudflare Logpush access records into hourly Parquet (#8).

The raw NDJSON objects stay untouched as the system of record (ADR 0002); this is
derived and rebuildable. Querying the raw objects directly does not scale: a day is
~2,600 objects and ~16 GB of JSON, the access record is a JSON *string* inside the
Workers trace envelope (`Logs[].Message[]`), and the stream also carries non-JSON
`waitUntil()` warnings and scheduled-event traces. Parse once, here.

Output is one Parquet file per UTC hour of the record's `ts`, sorted by `ts`, zstd:

    <out>/year=YYYY/month=M/day=D/hour=H/logs.parquet

The first 33 columns are the CloudFront Parquet's, same names, order and types, so
both eras UNION ALL (sql/access.sql). Then `ts`, the `cf_*` extras, `cf` (the verbatim
Cloudflare object, JSON text) and `client_id` = sha256(salt || c_ip) (ADR 0012).
Only `type = 'access'` messages are kept; what was skipped is counted in the log.

Usage:
    ./cloudflare-logs-to-parquet.py --self-check
    ./cloudflare-logs-to-parquet.py --from 2026-09-29 --to 2026-09-29      # a UTC day
    ./cloudflare-logs-to-parquet.py --from 2026-09-29T05 --to 2026-09-29T07
    ./cloudflare-logs-to-parquet.py --recent --upload     # the 15-min timer
    ./cloudflare-logs-to-parquet.py --yesterday --upload  # the nightly re-seal
    ./cloudflare-logs-to-parquet.py --verify --from 2026-09-29 --to 2026-09-29

Rewriting an hour is idempotent: the file is written aside and renamed over the old
one. A run builds, then verifies (access records per hour in the raw objects = rows
in the Parquet), and only then, with --upload, copies the hours to R2.

Hours are processed a UTC day at a time, reading every object that can hold one of
their records — see LATE for why that reaches hours past the day.
"""

import argparse, datetime as dt, json, os, pathlib, re, runpy, subprocess, sys, tempfile, time

HERE = pathlib.Path(__file__).resolve().parent
# One list of CloudFront fields, not two: the column parity is the point.
FIELDS = runpy.run_path(str(HERE / "cloudfront-logs-to-parquet.py"))["FIELDS"]

# Where the raw objects are: an rclone path (GCS today, R2 after bioc-edge#22) or a
# local directory holding them, flat or under {YYYYMMDD}/.
SOURCE = "gs1:bioc-u24-logs/cloudflare/bioc-access-logs"
OUT = pathlib.Path("/data/davsean/bioc-cloudflare-parquet")
R2_DEST = "r2:bioc-access-logs/parquet/cloudflare"
SCRATCH = pathlib.Path("/data/davsean/tmp")   # /tmp is small on onclappc02
SALT_SECRET = ["--secret", "bioc-logs-ip-salt", "--project", "cdsci-infra"]

HOUR = dt.timedelta(hours=1)
# An object named {start}_{end}_{hash} holds records with ts up to ~1 s after `end`,
# and up to hours *before* `start`: the trace event is emitted when the invocation
# finishes, so a long streamed download is delivered long after its record's ts.
# ponytail: measured over 24 h (2026-09-29/30): median 33 s, max 3.0 h, 33 of 6.3M
# records over 1 h. Records later than LATE are counted in the log ("late"), not kept;
# raise LATE if that count is ever non-zero.
EARLY = dt.timedelta(minutes=1)
LATE = dt.timedelta(hours=6)

NAME = re.compile(r"(\d{8}T\d{6}Z)_(\d{8}T\d{6}Z)_[0-9a-f]+\.log\.gz$")

# The record's shape (bioc-edge worker/src/keys.ts accessRecord). Numbers land in the
# CloudFront columns as their text ('200', '174984'), matching the VARCHAR mirror.
EXTRAS = {"cf_country": "VARCHAR", "cf_continent": "VARCHAR", "cf_asn": "BIGINT",
          "cf_as_organization": "VARCHAR", "cf_client_tcp_rtt": "BIGINT",
          "cf_client_accept_encoding": "VARCHAR"}
RECORD = {"type": "VARCHAR", "v": "INTEGER", "ts": "BIGINT",
          **{f: "VARCHAR" for f in FIELDS if f not in ("date", "time")},
          **EXTRAS, "cf": "JSON"}

# ADR 0012. The salt is the 64-char hex text, set per connection, never written down.
CLIENT_ID = "sha256(getvariable('ip_salt') || r.c_ip)"

# The BigQuery view's conversions (sql/cloudflare_access.bq.sql), ported.
CONVERT = {
    "date": "CAST(ts AS DATE)",
    "time": "strftime(ts, '%H:%M:%S')",
    # Worker logs url.search ("?a=b"); CloudFront logs the bare query. Strip the "?".
    "cs_uri_query": "NULLIF(ltrim(r.cs_uri_query, '?'), '')",
    # Worker records milliseconds; CloudFront's column is seconds ("0.002").
    "time_taken": "printf('%.3f', r.time_taken::BIGINT / 1000)",
}
COLUMNS = ",\n       ".join(
    [f"{CONVERT.get(f, 'r.' + f)} AS {f}" for f in FIELDS] + ["ts"]
    + [f"r.{c} AS {c}" for c in EXTRAS] + ["r.cf::VARCHAR AS cf", f"{CLIENT_ID} AS client_id"])



def raw_messages(files):
    """SQL: one row per log message in the objects: (obj_start, msg, type, v, ts).

    type/v/ts are NULL for non-JSON messages. `->>` on those is an error, not a NULL,
    and DuckDB does not short-circuit AND, hence the CASEs.

    Objects fetched with rclone keep their gzip bytes; `gcloud storage cp` decompresses
    them (transcoding) under the same .log.gz name. Sniff rather than trust the name.
    """
    by_codec = {}
    for f in files:
        with open(f, "rb") as fh:
            by_codec.setdefault("gzip" if fh.read(2) == b"\x1f\x8b" else "uncompressed", []).append(str(f))
    reads = " UNION ALL ".join(
        f"SELECT json, filename FROM read_ndjson_objects({fs!r}, compression='{c}', filename=true)"
        for c, fs in by_codec.items())
    return f"""
        SELECT obj_start, msg,
               CASE WHEN json_valid(msg) THEN msg->>'type' END AS type,
               CASE WHEN json_valid(msg) THEN msg->>'v' END AS v,
               CASE WHEN json_valid(msg) THEN epoch_ms((msg->>'ts')::BIGINT) END AS ts
        FROM (SELECT strptime(regexp_extract(filename, '(\\d{{8}}T\\d{{6}}Z)_\\d{{8}}T\\d{{6}}Z_[0-9a-f]+\\.log\\.gz$', 1),
                        '%Y%m%dT%H%M%SZ') AS obj_start,
               unnest(from_json(json->'$.Logs[*].Message[*]', '["VARCHAR"]')) AS msg
        FROM ({reads}))"""


def ts_between(start, end):
    return f"ts >= TIMESTAMP '{start:%Y-%m-%d %H:%M:%S}' AND ts < TIMESTAMP '{end:%Y-%m-%d %H:%M:%S}'"


def hour_path(out, h):
    return out / f"year={h.year}" / f"month={h.month}" / f"day={h.day}" / f"hour={h.hour}" / "logs.parquet"


def hours(start, end):
    h = start
    while h < end:
        yield h
        h += HOUR


def list_objects(source, start, end):
    """Objects, relative to source, that can hold records with ts in [start, end)."""
    if os.path.isdir(source):
        names = [str(p.relative_to(source)) for p in pathlib.Path(source).rglob("*.log.gz")]
    else:
        # Logpush files an object under the UTC date of its `end`.
        names, today = [], dt.datetime.now(dt.UTC).replace(tzinfo=None).date()
        day, last = (start - EARLY).date(), min((end + LATE + 10 * EARLY).date(), today)
        while day <= last:
            ls = subprocess.run(["rclone", "lsf", "--files-only", f"{source}/{day:%Y%m%d}/"],
                                capture_output=True, text=True, check=True)
            names += [f"{day:%Y%m%d}/{n}" for n in ls.stdout.split()]
            day += dt.timedelta(days=1)
    keep = []
    for n in names:
        m = NAME.search(n)
        if m:
            s, e = (dt.datetime.strptime(x, "%Y%m%dT%H%M%SZ") for x in m.groups())
            if e >= start - EARLY and s < end + LATE:
                keep.append(n)
    return sorted(keep)


def fetch(source, names, into):
    """Local paths for the objects, copying them down with rclone if source is remote."""
    if os.path.isdir(source):
        return [pathlib.Path(source) / n for n in names]
    lst = into / "objects.txt"
    lst.write_text("\n".join(names) + "\n")
    subprocess.run(["rclone", "copy", "-q", "--files-from-raw", str(lst), "--transfers", "64",
                    "--checkers", "64", source, str(into / "raw")], check=True)
    return [into / "raw" / n for n in names]


def ip_salt():
    s = subprocess.run(["gcloud", "secrets", "versions", "access", "latest", *SALT_SECRET],
                       capture_output=True, text=True, check=True).stdout.rstrip("\n")
    if not re.fullmatch(r"[0-9a-f]{64}", s):
        raise SystemExit("bioc-logs-ip-salt is not 64 hex characters; refusing to hash with it")
    return s


def build(con, files, start, end, out):
    """Write every hour in [start, end) from the objects. Returns rows per hour."""
    if not files:
        print("  no objects", flush=True)
        return {}
    con.execute(f"CREATE OR REPLACE TEMP TABLE msg AS {raw_messages(files)}")
    n, bad_json, other, access, not_v1, in_range, late = con.execute(f"""
        SELECT count(*), count(*) FILTER (type IS NULL), count(*) FILTER (type <> 'access'),
               count(*) FILTER (type = 'access'),
               count(*) FILTER (type = 'access' AND v IS DISTINCT FROM '1'),
               count(*) FILTER (type = 'access' AND {ts_between(start, end)}),
               count(*) FILTER (type = 'access' AND
                   obj_start - ts > INTERVAL {int(LATE.total_seconds())} SECOND)
        FROM msg""").fetchone()
    print(f"  {len(files):,d} objects, {n:,d} messages: {access:,d} access ({in_range:,d} in range), "
          f"skipped {bad_json:,d} non-JSON + {other:,d} non-access; {late:,d} late beyond {LATE}",
          flush=True)
    # The mapping below assumes record v1. A new version would map silently, so stop.
    if not_v1:
        raise SystemExit(f"{not_v1:,d} access records are not v1 — update RECORD/COLUMNS first")
    con.execute(f"CREATE OR REPLACE TEMP TABLE rec AS SELECT {COLUMNS} FROM ("
                f"SELECT epoch_ms(r.ts) AS ts, r FROM (SELECT from_json(msg, '{json.dumps(RECORD)}') AS r "
                f"FROM msg WHERE type = 'access' AND {ts_between(start, end)}))")
    con.execute("DROP TABLE msg")
    written = {}
    for h in hours(start, end):
        target = hour_path(out, h)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".parquet.partial")
        con.execute(f"COPY (SELECT * FROM rec WHERE {ts_between(h, h + HOUR)} ORDER BY ts, x_edge_request_id) "
                    f"TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)")
        tmp.replace(target)
        written[h] = con.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]
    con.execute("DROP TABLE rec")
    return written


def verify(con, files, start, end, out):
    """Access records per hour in the raw objects vs rows in the Parquet. Returns #bad.

    Counts the raw side independently of the projection that wrote the Parquet, so a
    short or stale file (late objects since it was written) shows up as a mismatch.
    """
    raw = dict(con.execute(f"""
        SELECT date_trunc('hour', ts) h, count(*)
        FROM ({raw_messages(files)}) WHERE type = 'access' AND {ts_between(start, end)}
        GROUP BY h""").fetchall()) if files else {}
    bad = 0
    for h in hours(start, end):
        p = hour_path(out, h)
        n_pq = con.execute(f"SELECT count(*) FROM read_parquet('{p}')").fetchone()[0] if p.exists() else 0
        n_raw = raw.get(h, 0)
        ok = n_raw == n_pq
        bad += not ok
        print(f"  {h:%Y-%m-%dT%H}  raw {n_raw:>9,d}  parquet {n_pq:>9,d}  "
              f"{'OK' if ok else f'MISMATCH {n_pq - n_raw:+,d}'}", flush=True)
    return bad


def upload(out, start, end, r2, dry_run):
    rel = [str(hour_path(out, h).relative_to(out)) for h in hours(start, end) if hour_path(out, h).exists()]
    with tempfile.NamedTemporaryFile("w", dir=SCRATCH, suffix=".txt") as lst:
        lst.write("\n".join(rel) + "\n")
        lst.flush()
        subprocess.run(["rclone", "copy", "--files-from-raw", lst.name, str(out), r2,
                        *(["--dry-run"] if dry_run else [])], check=True)
    print(f"  uploaded {len(rel)} hour(s) -> {r2}{' (dry run)' if dry_run else ''}", flush=True)


def parse_bound(s, is_end):
    """'YYYY-MM-DD' or 'YYYY-MM-DDTHH' (UTC) -> an hour boundary; --to is inclusive."""
    if "T" in s:
        h = dt.datetime.strptime(s, "%Y-%m-%dT%H")
        return h + HOUR if is_end else h
    d = dt.datetime.strptime(s, "%Y-%m-%d")
    return d + dt.timedelta(days=1) if is_end else d


def day_chunks(start, end):
    """[start, end) split at UTC midnight: the unit of reading, writing and uploading."""
    while start < end:
        nxt = min(dt.datetime.combine(start.date() + dt.timedelta(days=1), dt.time()), end)
        yield start, nxt
        start = nxt


def self_check():
    """client_id test vector, and the projection against a synthetic envelope."""
    import duckdb, gzip
    con = duckdb.connect()
    con.execute("SET VARIABLE ip_salt = 'deadbeef'")
    got = con.execute(f"SELECT {CLIENT_ID} FROM (SELECT '203.0.113.7' AS c_ip) r").fetchone()[0]
    assert got == "9134dc805ff5ba704ac2324afa902b2f4510045b7b25e0cbfa4f1699d031a2f2", got

    rec = {"type": "access", "v": 1, "ts": 1790706037461, "c_ip": "203.0.113.7", "sc_status": 206,
           "cs_uri_query": "?a=b", "time_taken": 658, "sc_range_start": 0, "cs_cookie": None,
           "cf_asn": 140903, "cf": {"colo": "LHR", "asn": 140903}}
    fetch_ev = {"EventType": "fetch", "Logs": [{"Message": [json.dumps(rec)]},
                {"Message": ["waitUntil() tasks did not complete within the allowed time"]}]}
    cron_ev = {"EventType": "scheduled", "Logs": [{"Message": ['{"type":"sync","ok":true}']}]}
    with tempfile.TemporaryDirectory() as d:
        d = pathlib.Path(d)
        # One plain (gcloud-transcoded) and one gzip (rclone) object, same names as Logpush's.
        (d / "20260929T182206Z_20260929T182308Z_19c6c50c.log.gz").write_text(json.dumps(fetch_ev) + "\n")
        with gzip.open(d / "20260929T182309Z_20260929T182414Z_d274b24d.log.gz", "wt") as f:
            f.write(json.dumps(fetch_ev) + "\n" + json.dumps(cron_ev) + "\n")
        start = dt.datetime(2026, 9, 29, 18)
        files = [d / n for n in list_objects(str(d), start, start + HOUR)]
        assert len(files) == 2, files
        out = d / "out"
        assert build(con, files, start, start + HOUR, out) == {start: 2}
        assert verify(con, files, start, start + HOUR, out) == 0
        cur = con.execute(f"SELECT * FROM read_parquet('{hour_path(out, start)}', hive_partitioning=false)")
        cols = [c[0] for c in cur.description]
        r = dict(zip(cols, cur.fetchone()))
    assert cols[:33] == FIELDS, cols
    assert cols[33:] == ["ts", *EXTRAS, "cf", "client_id"], cols
    assert (str(r["date"]), r["time"], r["sc_status"], r["cs_uri_query"], r["time_taken"],
            r["sc_range_start"], r["cs_cookie"], r["cf_asn"]) == \
        ("2026-09-29", "18:20:37", "206", "a=b", "0.658", "0", None, 140903), r
    assert json.loads(r["cf"]) == rec["cf"], r["cf"]
    assert r["client_id"] == got
    print(f"self-check OK (client_id test vector; {len(cols)} columns, first 33 = CloudFront's)")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from", dest="start", help="first UTC day or hour, YYYY-MM-DD[THH]")
    ap.add_argument("--to", dest="end", help="last UTC day or hour, inclusive")
    ap.add_argument("--recent", action="store_true", help="the current and previous UTC hour")
    ap.add_argument("--yesterday", action="store_true", help="all of yesterday (UTC)")
    ap.add_argument("--verify", action="store_true", help="only reconcile counts, raw vs Parquet")
    ap.add_argument("--upload", action="store_true", help="rclone the verified hours to --r2")
    ap.add_argument("--dry-run", action="store_true", help="pass --dry-run to the R2 upload")
    ap.add_argument("--source", default=SOURCE, help=f"rclone path or local dir (default {SOURCE})")
    ap.add_argument("--out", default=OUT, type=pathlib.Path, help=f"local Parquet (default {OUT})")
    ap.add_argument("--r2", default=R2_DEST, help=f"upload destination (default {R2_DEST})")
    ap.add_argument("--self-check", action="store_true", help="run the built-in checks and exit")
    a = ap.parse_args()

    if a.self_check:
        return self_check()
    now = dt.datetime.now(dt.UTC).replace(tzinfo=None, minute=0, second=0, microsecond=0)
    if a.recent:
        start, end = now - HOUR, now + HOUR
    elif a.yesterday:
        end = now.replace(hour=0)
        start = end - dt.timedelta(days=1)
    elif a.start and a.end:
        start, end = parse_bound(a.start, False), parse_bound(a.end, True)
    else:
        ap.error("give --from and --to, --recent, --yesterday, or --self-check")

    import duckdb
    con, bad = duckdb.connect(), 0
    # Per process: the 15-min and nightly runs can overlap.
    con.execute(f"SET temp_directory = '{SCRATCH / f'duckdb-tmp-{os.getpid()}'}'")
    if not a.verify:
        con.execute("SET VARIABLE ip_salt = ?", [ip_salt()])
    for c_start, c_end in day_chunks(start, end):
        t0 = time.time()
        print(f"{c_start:%Y-%m-%dT%H} .. {c_end - HOUR:%Y-%m-%dT%H}", flush=True)
        names = list_objects(a.source, c_start, c_end)
        with tempfile.TemporaryDirectory(dir=SCRATCH, prefix="cf-parquet-") as d:
            files = fetch(a.source, names, pathlib.Path(d))
            if not a.verify:
                build(con, files, c_start, c_end, a.out)
            n_bad = verify(con, files, c_start, c_end, a.out)
        bad += n_bad
        if a.upload and not a.verify:
            if n_bad:
                print("  not uploaded: verify failed", flush=True)
            else:
                upload(a.out, c_start, c_end, a.r2, a.dry_run)
        print(f"  {time.time() - t0:.0f}s", flush=True)
    print(f"{bad} hour(s) mismatched")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
