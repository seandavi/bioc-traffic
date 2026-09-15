#!/usr/bin/env python3
"""Mirror the raw CloudFront access logs for bioconductor.org into Parquet.

Background: the published download stats at /packages/stats/ are produced by two
separate pipelines that currently disagree (see docs/download-stats.qmd). Both are
fed by these logs, and we have direct read access to them. This makes a local,
queryable copy so any question about the numbers can be settled without depending on
either pipeline — or on their Athena setup, which we have no permission to use.

This copies the logs. It does not interpret them. No rows are filtered and no columns
are dropped: all 33 CloudFront fields, every request, exactly as logged (VARCHAR
throughout, except `date`). Interpretation happens locally, as views over this
mirror — see DOWNLOADS_SQL below.

That split is deliberate, and it is the lesson from the two existing pipelines. Both
filter at ingest, keeping only package-tarball requests and a handful of columns. So
neither can answer a question its authors did not anticipate — whether redirects
should really count as downloads, whether bots can be excluded, what the 206
responses look like — without re-reading six years of logs from S3. The egress is
paid once; what you keep afterwards is free. Keep everything.

Usage:
    ./cloudfront-logs-to-parquet.py --self-check         # verify the downloads view
    ./cloudfront-logs-to-parquet.py --from 2026-06 --to 2026-07
    ./cloudfront-logs-to-parquet.py --from 2020-01 --to 2026-08 --out /data/cf

Output is one Parquet file per month, Hive-partitioned as year=YYYY/month=M/, so it
queries as a single table with pruning on either level:

    duckdb -c "select count(*) from read_parquet('/data/cf/**/*.parquet',
                    hive_partitioning=true) where year = 2026 and month = 7"

Rows arrive in roughly chronological order (the S3 keys sort by date and hour), so
Parquet row-group statistics on `date` prune well without an explicit sort or
day-level partitioning. Add day partitioning only if that stops being true.

Note this retains client IP addresses for all site traffic, not only for downloads —
a broader personal-data footprint than the published statistics imply. Worth a
deliberate decision about retention and access before it is copied anywhere else.

Caveat: CloudFront logs start 2020-01-01. Statistics before that exist only in the
legacy pipeline's own databases and cannot be regenerated from anything.
"""

import argparse, pathlib, sys, time

BUCKET = "bioc-cloudfront-logs"
# CloudFront names every object <distribution>.<YYYY-MM-DD-HH>.<hash>.gz, flat at the
# bucket root — there are no prefixes to narrow on except the date in the name.
DISTRIBUTION = "E1TVLJONPTUXV3"

# The 33 W3C fields, in the order CloudFront emits them (the '#Fields:' header line).
FIELDS = [
    "date", "time", "x_edge_location", "sc_bytes", "c_ip", "cs_method", "cs_host",
    "cs_uri_stem", "sc_status", "cs_referer", "cs_user_agent", "cs_uri_query",
    "cs_cookie", "x_edge_result_type", "x_edge_request_id", "x_host_header",
    "cs_protocol", "cs_bytes", "time_taken", "x_forwarded_for", "ssl_protocol",
    "ssl_cipher", "x_edge_response_result_type", "cs_protocol_version", "fle_status",
    "fle_encrypted_fields", "c_port", "time_to_first_byte",
    "x_edge_detailed_result_type", "sc_content_type", "sc_content_len",
    "sc_range_start", "sc_range_end",
]

_COLS = ",\n       ".join(
    (f"CAST(column{i:02d} AS DATE) AS {n}" if n == "date" else f"column{i:02d} AS {n}")
    for i, n in enumerate(FIELDS))

SELECT_SQL = f"SELECT {_COLS}\nFROM {{src}}"

# DO NOT set comment='#'. DuckDB treats '#' as a comment marker mid-line, not only
# at line start, so any record whose user-agent, referer or URI contains a '#' is
# truncated at that point — a Sogou crawler UA ending "webmasters.htm#07" truncates
# to exactly 11 fields. Combined with ignore_errors=true the malformed rows are then
# dropped silently: one 9,168-line file yielded 93 rows. Header lines are filtered in
# SQL instead (see HEADERLESS), and null_padding lets the 1-field '#Version'/'#Fields'
# lines parse so they can be filtered rather than derailing the sniffer.
READ_CSV = ("read_csv('{glob}', delim='\\t', header=false, "
            "all_varchar=true, null_padding=true, ignore_errors=true)")

# The two '#Version:' / '#Fields:' lines at the top of every object.
HEADERLESS = "(SELECT * FROM {read} WHERE column00 NOT LIKE '#%')"


def source_glob(month, logs_dir=None):
    """Where to read a month from: a local mirror of the bucket, or S3 directly.

    Reading S3 is latency-bound on ~30k small objects per month; off local disk the
    same conversion is CPU-bound and several times faster. If the logs have already
    been copied down (rclone, --transfers 128), point at them.
    """
    name = f"{DISTRIBUTION}.{month}-*.gz"
    return f"{str(logs_dir).rstrip('/')}/{name}" if logs_dir else f"s3://{BUCKET}/{name}"

# ---------------------------------------------------------------------------
# Interpretation lives here, as a view over the mirror — not in the extract.
#
# This reproduces what BOTH published pipelines count as a "download", so results are
# comparable to /packages/stats/ and /packages/oldstats/: a package tarball or binary
# under /packages/, status 200/301/302/307/308, HEAD excluded. Redirects count and 206
# (partial content) does not — that is their convention, not an endorsement. Measured
# to reproduce the published bioc figures to within 0.15%.
#
# It deliberately does NOT filter bots. Neither published series does, and both
# discarded the user-agent at ingest so neither now can. Here cs_user_agent is
# present, so that policy is a WHERE clause away rather than a re-download away.
# ---------------------------------------------------------------------------
PKG_URI = r'^/+packages/+[^/]+/+(bioc|workflows|data/+experiment|data/+annotation)/+(bin|src)/+.*_.*\.(tar\.gz|zip|tgz)$'

DOWNLOADS_SQL = f"""
CREATE OR REPLACE VIEW downloads AS
SELECT *,
       regexp_extract(cs_uri_stem, '{PKG_URI}', 1) AS category,
       regexp_extract(cs_uri_stem, '/([^/_]+)_[^/]*\\.(tar\\.gz|zip|tgz)$', 1) AS package
FROM {{src}}
WHERE sc_status IN ('200','301','302','307','308')
  AND cs_method <> 'HEAD'
  AND regexp_matches(cs_uri_stem, '{PKG_URI}')
"""


def connect(need_s3=True):
    import duckdb
    con = duckdb.connect()
    if need_s3:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1')")
    return con


def extract_month(con, month, out_dir, logs_dir=None):
    """Mirror one month of logs to out_dir/year=YYYY/month=M/logs.parquet.

    DuckDB streams the COPY, so a month of logs does not need to fit in memory.
    """
    year, mon = month.split("-")
    dest = out_dir / f"year={year}" / f"month={int(mon)}"
    target = dest / "logs.parquet"
    if target.exists():          # resume: a 6-year backfill will be interrupted
        return None
    dest.mkdir(parents=True, exist_ok=True)
    src = HEADERLESS.format(read=READ_CSV.format(glob=source_glob(month, logs_dir)))
    # The mapping below is positional, so a format change upstream would silently shift
    # every column. Verified identical 2020-2026, but assert it per month rather than
    # trust it. Note this only samples the schema; it cannot catch per-row truncation,
    # which is why the row-count reconciliation in --verify exists.
    n_src = len(con.execute(f"SELECT * FROM {src} LIMIT 0").description)
    if n_src != len(FIELDS):
        raise SystemExit(f"{month}: expected {len(FIELDS)} CloudFront fields, source has "
                         f"{n_src} — the log format changed; fix FIELDS before continuing")
    # Write to a temp name first so an interrupted run doesn't leave a short file
    # that the resume check would then skip.
    tmp = dest / "logs.parquet.partial"
    con.execute(f"COPY ({SELECT_SQL.format(src=src)}) TO '{tmp}' "
                "(FORMAT parquet, COMPRESSION zstd)")
    tmp.rename(target)
    return con.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]


def verify_month(con, month, out_dir, logs_dir=None):
    """Reconcile the Parquet row count against the source for one month.

    This exists because the failure that motivated it was silent. `comment='#'`
    truncated any record containing a '#' and `ignore_errors=true` then dropped the
    malformed rows without a word — one file yielded 93 rows out of 9,168, and the
    build carried on reporting success. A schema assertion cannot catch that: the
    columns are right, the rows are missing. Only counting both sides can.
    """
    year, mon = month.split("-")
    target = out_dir / f"year={year}" / f"month={int(mon)}" / "logs.parquet"
    if not target.exists():
        return None
    src = HEADERLESS.format(read=READ_CSV.format(glob=source_glob(month, logs_dir)))
    n_src = con.execute(f"SELECT count(*) FROM {src}").fetchone()[0]
    n_pq = con.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]
    return n_src, n_pq


def months(start, end):
    """Inclusive YYYY-MM range."""
    y, m = (int(x) for x in start.split("-"))
    ey, em = (int(x) for x in end.split("-"))
    while (y, m) <= (ey, em):
        yield f"{y:04d}-{m:02d}"
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def self_check():
    """The mirror is a straight copy; the downloads view is the part that can drift."""
    import duckdb
    con = duckdb.connect()
    # (uri, status, method, should_match, expected_package)
    cases = [
        ("/packages/3.20/bioc/src/contrib/limma_3.62.1.tar.gz", "200", "GET", True, "limma"),
        ("/packages/3.20/bioc/bin/windows/contrib/4.4/limma_3.62.1.zip", "200", "GET", True, "limma"),
        ("/packages/3.20/data/annotation/src/contrib/org.Hs.eg.db_3.20.0.tar.gz", "200", "GET", True, "org.Hs.eg.db"),
        ("/packages/3.20/bioc/src/contrib/limma_3.62.1.tar.gz", "302", "GET", True, "limma"),
        ("/packages/3.20/bioc/src/contrib/limma_3.62.1.tar.gz", "404", "GET", False, None),
        ("/packages/3.20/bioc/src/contrib/limma_3.62.1.tar.gz", "206", "GET", False, None),
        ("/packages/3.20/bioc/src/contrib/limma_3.62.1.tar.gz", "200", "HEAD", False, None),
        ("/packages/3.20/bioc/html/limma.html", "200", "GET", False, None),
        ("/packages/3.20/bioc/src/contrib/PACKAGES", "200", "GET", False, None),
        ("/about/index.html", "200", "GET", False, None),
    ]
    # A stand-in mirror with the real column names, so the view binds exactly as it
    # will against the Parquet.
    rows = ",".join(f"('{u}','{s}','{m}')" for u, s, m, _, _ in cases)
    con.execute(f"CREATE VIEW mirror AS SELECT * FROM (VALUES {rows}) "
                "AS v(cs_uri_stem, sc_status, cs_method)")
    con.execute(DOWNLOADS_SQL.format(src="mirror"))

    cur = con.execute("SELECT * FROM downloads")
    cols = [d[0] for d in cur.description]
    got = [dict(zip(cols, r)) for r in cur.fetchall()]

    failures = []
    matched = {r["package"] for r in got}
    for uri, status, method, should, pkg in cases:
        if should and pkg not in matched:
            failures.append(f"expected match, got none: {method} {status} {uri}")
    n_expected = sum(1 for c in cases if c[3])
    if len(got) != n_expected:
        failures.append(f"expected {n_expected} matching rows, got {len(got)}")
    for r in got:                                   # extraction must not yield blanks
        if not r["package"] or not r["category"]:
            failures.append(f"empty package/category for {r['cs_uri_stem']}")
    # The mirror must stay a faithful copy: no row filter, no derived columns.
    up = SELECT_SQL.upper()
    if "WHERE" in up or "REGEXP" in up:
        failures.append("SELECT_SQL has grown a filter — the mirror must stay raw")
    if len(FIELDS) != 33:
        failures.append(f"expected 33 CloudFront fields, have {len(FIELDS)}")

    for f in failures:
        print("FAIL:", f, file=sys.stderr)
    assert not failures, f"{len(failures)} self-check failure(s)"
    print(f"self-check OK (downloads view: {n_expected} matched, "
          f"{len(cases) - n_expected} rejected; mirror: {len(FIELDS)} fields, unfiltered)")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from", dest="start", help="first month, YYYY-MM")
    ap.add_argument("--to", dest="end", help="last month, YYYY-MM (inclusive)")
    ap.add_argument("--out", default="./cloudfront-parquet", type=pathlib.Path)
    ap.add_argument("--logs-dir", type=pathlib.Path,
                    help="read from a local mirror of the bucket instead of S3")
    ap.add_argument("--self-check", action="store_true",
                    help="verify the downloads view and exit")
    ap.add_argument("--verify", action="store_true",
                    help="reconcile Parquet row counts against the source and exit")
    a = ap.parse_args()

    if a.self_check:
        return self_check()
    if not (a.start and a.end):
        ap.error("--from and --to are required (or use --self-check)")

    if a.verify:
        con, bad = connect(need_s3=a.logs_dir is None), 0
        for month in months(a.start, a.end):
            r = verify_month(con, month, a.out, a.logs_dir)
            if r is None:
                print(f"{month}  (not built)")
                continue
            n_src, n_pq = r
            ok = n_src == n_pq
            bad += not ok
            print(f"{month}  source {n_src:>12,d}  parquet {n_pq:>12,d}  "
                  f"{'OK' if ok else f'MISMATCH {n_pq - n_src:+,d}'}", flush=True)
        print(f"\n{bad} month(s) mismatched")
        return 1 if bad else 0

    con, total = connect(need_s3=a.logs_dir is None), 0
    for month in months(a.start, a.end):
        t0 = time.time()
        n = extract_month(con, month, a.out, a.logs_dir)
        note = "skipped (exists)" if n is None else f"{n:>12,d} rows  {time.time()-t0:5.0f}s"
        print(f"{month}  {note}", flush=True)
        total += n or 0
    print(f"total {total:,d} rows -> {a.out}")


if __name__ == "__main__":
    main()
