#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb>=1.4"]
# ///
"""Package download statistics across both eras, as static files (#11, ADR 0004).

    ./download-stats.py                 # rebuild stale months, then every output
    ./download-stats.py --all           # rebuild every month (after a client_class version bump)
    ./download-stats.py --self-check

Three stages, all under --out (default /data/davsean/bioc-traffic-stats):

1. clients/year=Y/month=M/clients.parquet — one row per (date, category, package,
   bioc_version, client_id, client_class, era) with its download count, from the
   `downloads` view (sql/downloads.sql), production hosts only. This is the only stage that
   reads the logs (a month of CloudFront is ~10 s); a month is rebuilt only when it is
   missing or a source Parquet file of that month is newer, so the CloudFront era is
   computed once and only months still receiving logs are recomputed. Internal: it
   carries client_id (never c_ip), so it is not published.

2. Aggregates, recomputed from the partitions every run:
     package_month.parquet   month × category × package, all months
     package_day.parquet     date × category × package, the last 90 days of data
     release_month.parquet   month × bioc_version
     category_month.parquet  month × category
     overall_month.parquet   month
   Columns: `downloads`, `distinct_clients` (the fixed definition), then per class group
   `downloads_<g>` / `distinct_clients_<g>` for g in human, package_client, automated
   (client_class_v0, interpretive), `rule_version`, and `era` (which eras contributed:
   'cloudfront', 'cloudflare' or 'cloudflare+cloudfront'). The Cloudflare era feeds the
   classifier cf_asn and verifiedBotCategory and the CloudFront era cannot, so the class
   columns step at the 2026-09-28 cutover for classifier reasons; the fixed ones do not.

3. stats/ — the /packages/stats/ tree (ADR 0004 contract) from the fixed columns:
     <cat>/<pkg>/<pkg>_stats.tab, <cat>/<pkg>/<pkg>_<year>_stats.tab
     <cat>/<p>_stats.tab, <cat>/<p>_<year>_stats.tab, <cat>/<p>_pkg_stats.tab,
     <cat>/<p>_pkg_scores.tab       (<p>: bioc, annotation, experiment, workflows)
   Formats match the live files: tab-separated, no trailing newline, a year block is its
   12 months plus an 'all' row whose distinct count is over the whole year (not a sum),
   years newest first, a package's years without downloads omitted, packages sorted
   case-insensitively. Download_score = floor(sum of monthly distinct clients over the
   12 complete months before the last data month / 12), for packages with any.
   Years start at 2020: the logs do, and earlier years cannot be regenerated.

Stages 2 and 3 count only (category, package) pairs a Bioconductor repository has listed:
the Package: fields of /packages/<v>/<cat>/src/contrib/PACKAGES for every release. The
CloudFront era answers any .../<cat>/src/contrib/<x>_<v>.tar.gz with a 302, so without
this, workflows alone gains ~1,800 names a month. The published tree applies the same
universe (it covers all 6,356 published pairs). The indexes are cached in
<out>/packages-index/; release and devel are refetched every run.
"""

import argparse, datetime as dt, os, pathlib, re, runpy, shutil, sys, tempfile, time, urllib.error, urllib.request

HERE = pathlib.Path(__file__).resolve().parent
OUT = pathlib.Path("/data/davsean/bioc-traffic-stats")
SCRATCH = pathlib.Path("/data/davsean/tmp")   # /tmp is small on onclappc02
CLOUDFRONT = pathlib.Path("/data/davsean/bioc-cf-parquet")
CLOUDFLARE = pathlib.Path("/data/davsean/bioc-cloudflare-parquet")
SQL = [HERE / "sql" / f for f in ("access.sql", "client_class.sql", "downloads.sql")]
SITE = "https://bioconductor.org"

# /packages/stats/ directory -> file prefix, and the repository path under /packages/<v>/.
CATEGORIES = {"bioc": ("bioc", "bioc"), "data-annotation": ("annotation", "data/annotation"),
              "data-experiment": ("experiment", "data/experiment"), "workflows": ("workflows", "workflows")}
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
# R package names; anything else in the log (URL junk, '..') never becomes a path.
PACKAGE_NAME = r"^[A-Za-z][A-Za-z0-9.]*[A-Za-z0-9]$"
DAYS = 90

# client_class_v0 groups for the interpretive columns. automated is everything else,
# unknown included.
GROUPS = {"human": "is_human_v0(client_class)",
          "package_client": "client_class = 'package_client'",
          "automated": "NOT is_human_v0(client_class) AND client_class <> 'package_client'"}
MEASURES = ",\n       ".join(
    ["sum(downloads)::BIGINT AS downloads", "count(DISTINCT client_id) AS distinct_clients"]
    + [m for g, cond in GROUPS.items() for m in (
        f"coalesce(sum(downloads) FILTER ({cond}), 0)::BIGINT AS downloads_{g}",
        f"count(DISTINCT client_id) FILTER ({cond}) AS distinct_clients_{g}")]
    + ["any_value(rule_version) AS rule_version",
       "string_agg(DISTINCT era, '+' ORDER BY era) AS era"])

# name -> (GROUP BY keys, WHERE). `month` is the first day of the month.
AGGREGATES = {
    "package_month": ("month, category, package", "true"),
    "package_day": ("date, category, package", f"date > (SELECT max(date) FROM repo_downloads) - {DAYS}"),
    "release_month": ("month, bioc_version", "true"),
    "category_month": ("month, category", "true"),
    "overall_month": ("month", "true"),
}


def ip_salt():
    import subprocess
    s = os.environ.get("BIOC_IP_SALT") or subprocess.run(
        ["gcloud", "secrets", "versions", "access", "latest", "--secret", "bioc-logs-ip-salt",
         "--project", "cdsci-infra"], capture_output=True, text=True, check=True).stdout
    s = s.rstrip("\n")
    if not re.fullmatch(r"[0-9a-f]{64}", s):
        raise SystemExit("bioc-logs-ip-salt is not 64 hex characters; refusing to hash with it")
    return s


def connect(salt=None):
    import duckdb
    con = duckdb.connect()
    tmp = SCRATCH / "duckdb-download-stats"
    tmp.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory = '{tmp}'")
    # ponytail: fixed caps for a shared, busy host (other DuckDB jobs run beside this one).
    con.execute("SET memory_limit = '40GB'; SET threads = 16")
    if salt:
        con.execute("SET VARIABLE ip_salt = ?", [salt])
    for f in SQL:
        con.execute(f.read_text())
    return con


def replace_file(con, query, target):
    """COPY to a sibling .partial, then rename: readers never see a short file."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".parquet.partial")
    con.execute(f"COPY ({query}) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)")
    tmp.replace(target)


# --- stage 1: monthly client partitions ------------------------------------------------

def partition(out, y, m):
    return out / "clients" / f"year={y}" / f"month={m}" / "clients.parquet"


def source_months():
    """(year, month) -> newest mtime of any source Parquet file of that month, both eras."""
    newest = {}
    for root in (CLOUDFRONT, CLOUDFLARE):
        for p in root.glob("year=*/month=*/**/logs.parquet"):
            y, m = (int(p.relative_to(root).parts[i].split("=")[1]) for i in (0, 1))
            newest[y, m] = max(newest.get((y, m), 0), p.stat().st_mtime)
    return newest


def build_partitions(con, out, rebuild_all):
    for (y, m), mtime in sorted(source_months().items()):
        target = partition(out, y, m)
        if not rebuild_all and target.exists() and target.stat().st_mtime > mtime:
            continue
        t0 = time.time()
        replace_file(con, f"""
            SELECT date, category, package, bioc_version, client_id, client_class, era,
                   rule_version, count(*) AS downloads
            FROM downloads WHERE production AND year = {y} AND month = {m}
            GROUP BY ALL""", target)
        n, d = con.execute(f"SELECT count(*), sum(downloads) FROM '{target}'").fetchone()
        print(f"  clients {y}-{m:02d}: {d or 0:,d} downloads, {n:,d} rows  {time.time() - t0:.0f}s",
              flush=True)


# --- the package universe --------------------------------------------------------------

def fetch(url):
    # bioconductor.org (Cloudflare) answers 403 to the default Python-urllib User-Agent.
    req = urllib.request.Request(url, headers={"User-Agent": "bioc-traffic download-stats"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise


def package_universe(out):
    """Write <out>/packages-index/*.txt and return the (category, package) rows."""
    config = fetch(f"{SITE}/config.yaml")
    dates = re.search(r"^release_dates:.*\n((?:[ \t]+.*\n)+)", config, re.M).group(1)
    versions = re.findall(r'^\s+"([0-9]+\.[0-9]+)":', dates, re.M)
    versions.append(re.search(r'^devel_version: "([0-9.]+)"', config, re.M).group(1))
    idx = out / "packages-index"
    idx.mkdir(parents=True, exist_ok=True)
    rows = []
    for v in versions:
        for cat, (_, path) in CATEGORIES.items():
            f = idx / f"{v}-{cat}.txt"
            # Releases before the last two are frozen; a 404 (no such repo) is cached as empty.
            if not f.exists() or v in versions[-2:]:
                text = fetch(f"{SITE}/packages/{v}/{path}/src/contrib/PACKAGES")
                f.write_text("" if text is None else
                             "\n".join(re.findall(r"^Package:\s*(\S+)", text, re.M)))
            rows += [(cat, p) for p in f.read_text().split()]
    return sorted(set(rows))


# --- stage 2: aggregates ---------------------------------------------------------------

def load_repo_downloads(con, out, universe):
    con.execute("CREATE OR REPLACE TEMP TABLE universe (category VARCHAR, package VARCHAR)")
    con.executemany("INSERT INTO universe VALUES (?, ?)", universe)
    con.execute(f"""
        CREATE OR REPLACE VIEW repo_downloads AS
        SELECT * REPLACE (make_date(year, month, 1) AS month)
        FROM read_parquet('{out}/clients/*/*/clients.parquet', hive_partitioning = true)
        SEMI JOIN universe USING (category, package)""")
    versions = [r[0] for r in con.execute("SELECT DISTINCT rule_version FROM repo_downloads").fetchall()]
    if len(versions) > 1:
        raise SystemExit(f"partitions mix client_class rule versions {versions}; rerun with --all")


def write_aggregates(con, out):
    for name, (keys, where) in AGGREGATES.items():
        t0 = time.time()
        replace_file(con, f"SELECT {keys},\n       {MEASURES}\nFROM repo_downloads WHERE {where}\n"
                          f"GROUP BY {keys} ORDER BY {keys}", out / f"{name}.parquet")
        n = con.execute(f"SELECT count(*) FROM '{out / name}.parquet'").fetchone()[0]
        print(f"  {name}: {n:,d} rows  {time.time() - t0:.0f}s", flush=True)


# --- stage 3: the /packages/stats/ tree ------------------------------------------------

def year_block(year, months, total):
    """13 rows: 12 months (0 where none) and 'all'. months: {month: (ips, n)}."""
    rows = [(str(year), MONTHS[m - 1], *months.get(m, (0, 0))) for m in range(1, 13)]
    return rows + [(str(year), "all", *total)]


def tab(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join("\t".join(map(str, r)) for r in [header, *rows]))


def write_stats(con, out):
    """Rebuild <out>/stats beside the old tree, then swap it in."""
    t0 = time.time()
    as_of = con.execute("SELECT max(date) FROM repo_downloads").fetchone()[0]
    first = con.execute("SELECT min(year) FROM repo_downloads").fetchone()[0]
    # Everything the tree needs in one scan: per package and per category, per month and year.
    rows = con.execute(f"""
        SELECT category, package, year, month, count(DISTINCT client_id), sum(downloads)::BIGINT
        FROM repo_downloads WHERE regexp_matches(package, '{PACKAGE_NAME}')
        GROUP BY GROUPING SETS ((category, package, year, month), (category, package, year),
                                (category, year, month), (category, year))""").fetchall()
    months, years = {}, {}   # (category, package|None, year) -> {month: (ips, n)} / (ips, n)
    for cat, pkg, y, m, ips, n in rows:
        if m is None:
            years[cat, pkg, y] = (ips, n)
        else:
            months.setdefault((cat, pkg, y), {})[m.month] = (ips, n)

    # The 12 complete months before as_of's month, as (year, month).
    now = as_of.year * 12 + as_of.month - 1
    window = [(i // 12, i % 12 + 1) for i in range(now - 12, now)]
    new = out / "stats.partial"
    shutil.rmtree(new, ignore_errors=True)
    head = ("Year", "Month", "Nb_of_distinct_IPs", "Nb_of_downloads")
    for cat, (prefix, _) in CATEGORIES.items():
        d = new / cat
        all_years = []
        for y in range(as_of.year, first - 1, -1):
            block = year_block(y, months.get((cat, None, y), {}), years.get((cat, None, y), (0, 0)))
            tab(d / f"{prefix}_{y}_stats.tab", head, block)
            all_years += block
        tab(d / f"{prefix}_stats.tab", head, all_years)
        pkgs = sorted({p for c, p, _ in years if c == cat and p is not None}, key=lambda p: (p.lower(), p))
        pkg_rows, scores = [], []
        for p in pkgs:
            blocks = []
            for y in range(as_of.year, first - 1, -1):
                if (cat, p, y) not in years:
                    continue
                block = year_block(y, months[cat, p, y], years[cat, p, y])
                tab(d / p / f"{p}_{y}_stats.tab", head, block)
                blocks += block
            tab(d / p / f"{p}_stats.tab", head, blocks)
            pkg_rows += [(p, *r) for r in blocks]
            ips = sum(months.get((cat, p, y), {}).get(m, (0, 0))[0] for y, m in window)
            if ips:
                scores.append((p, ips // 12))
        tab(d / f"{prefix}_pkg_stats.tab", ("Package", *head), pkg_rows)
        tab(d / f"{prefix}_pkg_scores.tab", ("Package", "Download_score"), scores)
    n_files = sum(1 for _ in new.rglob("*.tab"))
    (new / "DATA_AS_OF").write_text(f"{as_of}\n")
    old = out / "stats.old"
    shutil.rmtree(old, ignore_errors=True)
    if (out / "stats").exists():
        (out / "stats").rename(old)
    new.rename(out / "stats")
    shutil.rmtree(old, ignore_errors=True)
    print(f"  stats/: {n_files:,d} files, data as of {as_of}  {time.time() - t0:.0f}s", flush=True)


def run(out, rebuild_all, salt):
    con = connect(salt)
    print("partitions:", flush=True)
    build_partitions(con, out, rebuild_all)
    print("aggregates:", flush=True)
    load_repo_downloads(con, out, package_universe(out))
    write_aggregates(con, out)
    write_stats(con, out)


# --- checks ----------------------------------------------------------------------------

def self_check():
    """downloads view vs DOWNLOADS_SQL, and aggregates + tab tree on a synthetic month."""
    pkg_uri = runpy.run_path(str(HERE / "cloudfront-logs-to-parquet.py"))["PKG_URI"]
    view = (HERE / "sql" / "downloads.sql").read_text()
    assert view.count(pkg_uri) == 2, "sql/downloads.sql PKG_URI differs from cloudfront-logs-to-parquet.py"

    import duckdb
    con = duckdb.connect()
    con.execute((HERE / "sql" / "client_class.sql").read_text())
    ua_r, ua_web = "R (4.6.1 x86_64-pc-linux-gnu x86_64 linux-gnu)", "Mozilla/5.0 (X11; Linux x86_64) Chrome/140.0 Safari/537.36"
    # (uri, status, method, ua, era, client, day): a stand-in `access` with the columns the view reads.
    cases = [
        ("/packages/3.22/bioc/src/contrib/limma_3.66.0.tar.gz", "302", "GET", ua_r, "cloudfront", "a", 5),
        ("/packages/3.22/bioc/src/contrib/limma_3.66.0.tar.gz", "200", "GET", ua_r, "cloudfront", "a", 6),
        ("//packages/release/bioc/bin/windows/contrib/4.6/limma_3.66.0.zip", "200", "GET", ua_web, "cloudflare", "b", 20),
        ("/packages/3.22/data/annotation/src/contrib/org.Hs.eg.db_3.22.0.tar.gz", "200", "GET", "Wget/1.25", "cloudflare", "c", 20),
        ("/packages/3.22/workflows/src/contrib/limma_3.66.0.tar.gz", "302", "GET", ua_r, "cloudfront", "a", 7),
        ("/packages/3.22/bioc/src/contrib/..%2F_1.tar.gz", "302", "GET", ua_r, "cloudfront", "a", 7),
        ("/packages/3.22/bioc/src/contrib/limma_3.66.0.tar.gz", "206", "GET", ua_r, "cloudflare", "a", 8),
        ("/packages/3.22/bioc/src/contrib/limma_3.66.0.tar.gz", "200", "HEAD", ua_r, "cloudflare", "a", 8),
        ("/packages/3.22/bioc/src/contrib/PACKAGES", "200", "GET", ua_r, "cloudflare", "a", 8),
    ]
    rows = ",".join(f"('{u}','{s}','{me}','{ua}','{e}','{c}',DATE '2026-02-{d:02d}')"
                    for u, s, me, ua, e, c, d in cases)
    con.execute(f"""CREATE VIEW access AS SELECT *, year(date) AS year, month(date) AS month,
        date::TIMESTAMP AS ts, true AS production, NULL::BIGINT AS cf_asn, NULL AS bot_category
        FROM (VALUES {rows}) v(cs_uri_stem, sc_status, cs_method, cs_user_agent, era, client_id, date)""")
    con.execute((HERE / "sql" / "downloads.sql").read_text())
    got = con.execute("SELECT category, package, bioc_version, client_class FROM downloads ORDER BY ALL").fetchall()
    assert got == [("bioc", "..%2F", "3.22", "package_client"), ("bioc", "limma", "3.22", "package_client"),
                   ("bioc", "limma", "3.22", "package_client"), ("bioc", "limma", "release", "human_browser"),
                   ("data-annotation", "org.Hs.eg.db", "3.22", "mirror"),
                   ("workflows", "limma", "3.22", "package_client")], got

    with tempfile.TemporaryDirectory(dir=SCRATCH if SCRATCH.exists() else None) as d:
        out = pathlib.Path(d)
        target = partition(out, 2026, 2)
        replace_file(con, """SELECT date, category, package, bioc_version, client_id, client_class,
                                    era, rule_version, count(*) AS downloads FROM downloads GROUP BY ALL""", target)
        # A January-2025 row for limma from another client, to check year blocks and the score window.
        extra = partition(out, 2025, 1)
        replace_file(con, """SELECT DATE '2025-01-03' AS date, 'bioc' AS category, 'limma' AS package,
                                    '3.20' AS bioc_version, 'z' AS client_id, 'human_browser' AS client_class,
                                    'cloudfront' AS era, 'v0' AS rule_version, 3 AS downloads""", extra)
        universe = [("bioc", "limma"), ("bioc", "..%2F"), ("data-annotation", "org.Hs.eg.db")]
        load_repo_downloads(con, out, universe)
        write_aggregates(con, out)
        pm = con.execute(f"""SELECT category, package, downloads, distinct_clients, downloads_human,
                                    downloads_package_client, downloads_automated, era
                             FROM '{out}/package_month.parquet' WHERE month = DATE '2026-02-01'
                             ORDER BY ALL""").fetchall()
        assert pm == [("bioc", "..%2F", 1, 1, 0, 1, 0, "cloudfront"),
                      ("bioc", "limma", 3, 2, 1, 2, 0, "cloudflare+cloudfront"),
                      ("data-annotation", "org.Hs.eg.db", 1, 1, 0, 0, 1, "cloudflare")], pm
        write_stats(con, out)
        s = out / "stats" / "bioc"
        limma = (s / "limma" / "limma_stats.tab").read_text()
        assert not limma.endswith("\n")
        lines = limma.split("\n")
        assert lines[0] == "Year\tMonth\tNb_of_distinct_IPs\tNb_of_downloads"
        assert lines[1:3] == ["2026\tJan\t0\t0", "2026\tFeb\t2\t3"], lines
        assert lines[13] == "2026\tall\t2\t3" and lines[14] == "2025\tJan\t1\t3" and lines[26] == "2025\tall\t1\t3"
        assert len(lines) == 27, "2026 and 2025 only; years without downloads omitted"
        assert (s / "limma" / "limma_2025_stats.tab").exists() and not (s / "limma" / "limma_2024_stats.tab").exists()
        assert not any(s.glob("..*")), "invalid package names never become paths"
        # as_of is 2026-02-20: the window is 2025-02 .. 2026-01, which misses both limma months.
        assert (s / "bioc_pkg_scores.tab").read_text() == "Package\tDownload_score"
        assert (s / "bioc_stats.tab").read_text().split("\n")[2] == "2026\tFeb\t2\t3"
        assert (s / "bioc_pkg_stats.tab").read_text().split("\n")[1] == "limma\t2026\tJan\t0\t0"
        assert not (out / "stats" / "workflows" / "limma").exists(), "not in the workflows universe"
    print("self-check OK (downloads view = DOWNLOADS_SQL filter; aggregates; stats tree format)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=OUT, type=pathlib.Path, help=f"output directory (default {OUT})")
    ap.add_argument("--all", action="store_true", help="rebuild every monthly partition")
    ap.add_argument("--self-check", action="store_true", help="run the built-in checks and exit")
    a = ap.parse_args()
    if a.self_check:
        return self_check()
    run(a.out, a.all, ip_salt())


if __name__ == "__main__":
    sys.exit(main())
