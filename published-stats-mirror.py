#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb>=1.4"]
# ///
"""The published /packages/stats/ files as a source, like the logs (#24).

    ./published-stats-mirror.py crawl [--date YYYY-MM-DD]   # land raw, resumable
    ./published-stats-mirror.py normalise [--date ...]      # raw -> Parquet
    ./published-stats-mirror.py compare                     # vs download-stats.py, markdown
    ./published-stats-mirror.py --self-check

1. crawl walks https://bioconductor.org/packages/stats/ and /packages/oldstats/ (the
   legacy generator's tree, frozen at its last run) from their index pages, following
   every link that stays inside the tree: category pages, package pages, every .tab, .png,
   .css and .js. The trees are generated HTML, not directory listings, so the HTML is the
   map. Files land gzipped under <root>/raw/<date>/<URL path>.gz, `zcat` giving the exact
   bytes received (a directory URL is its index.html, which the server answers
   identically), with manifest.jsonl beside them: url, path, status, bytes and sha256 (of
   the uncompressed content), compressed_bytes, content_type, last_modified, fetched_at. A
   status other than 200 is recorded, body not kept (a missing path answers 308 to
   master.bioconductor.org). Network errors are retried with backoff and, if they
   persist, left out of the manifest so a rerun retries them: rerunning with the same
   --date resumes, re-reading the HTML already landed to rediscover links.

2. normalise parses every *_stats.tab of the snapshot into <root>/published_month.parquet
   (tree, category, package, year, month, distinct_ips, downloads, source, fetched_at;
   package NULL = the category total) and published_year.parquet (the 'all' rows: a
   year's distinct IPs are over the year, so they do not add). Monthly rows come from the
   per-year files; the all-years files and <p>_pkg_stats.tab are checked to repeat them.
   It fails on any .tab it cannot parse.

3. compare reads published_month (tree 'stats') and download-stats.py's package_month
   and category_month and prints a markdown report.

Re-snapshot: `just published-crawl` (today's date), then upload and normalise. Raw
snapshots are never edited; a new date is a new directory.
"""

import argparse, concurrent.futures as cf, datetime as dt, gzip, hashlib, json, pathlib, re, sys, time
import urllib.error, urllib.parse, urllib.request

HERE = pathlib.Path(__file__).resolve().parent
ROOT = pathlib.Path("/data/davsean/bioc-published-stats")
STATS = pathlib.Path("/data/davsean/bioc-traffic-stats")
SCRATCH = pathlib.Path("/data/davsean/tmp")
SITE = "https://bioconductor.org"
TREES = ("/packages/stats/", "/packages/oldstats/")
# Cloudflare answers 403 to Python-urllib's default User-Agent.
UA = "Mozilla/5.0 (compatible; bioc-traffic published-stats-mirror; +https://github.com/seandavi/bioc-traffic)"
WORKERS = 8
TRIES = 6
# Category files the site build reads (ADR 0004) but the stats/ index does not link for
# every category; fetched as seeds, and a missing one is recorded as such.
PREFIXES = {"bioc": "bioc", "data-annotation": "annotation", "data-experiment": "experiment",
            "workflows": "workflows"}
SEEDS = [f"{t}{c}/{p}_{kind}.tab" for t in TREES for c, p in PREFIXES.items()
         for kind in ("pkg_stats", "pkg_scores")]
LINK = re.compile(r"""(?:href|src)\s*=\s*["']([^"'#?]*)""", re.I)


# --- 1. crawl --------------------------------------------------------------------------

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None


OPENER = urllib.request.build_opener(NoRedirect)


def canonical(path):
    """A URL path inside one of TREES, directory URLs as their index.html; else None."""
    path = urllib.parse.unquote(path)
    if not path.startswith(TREES) or "/../" in path or "/./" in path:
        return None
    return path + "index.html" if path.endswith("/") else path


def links(base, body):
    out = set()
    for href in LINK.findall(body.decode("utf-8", "replace")):
        u = urllib.parse.urlsplit(urllib.parse.urljoin(SITE + base, href.strip()))
        if u.netloc == "bioconductor.org" and (p := canonical(u.path)):
            out.add(p)
    return out


def fetch(path):
    """(status, body, headers) for SITE+path, retrying network errors, 429 and 5xx."""
    for i in range(TRIES):
        req = urllib.request.Request(SITE + urllib.parse.quote(path), headers={"User-Agent": UA})
        try:
            with OPENER.open(req, timeout=120) as r:
                return r.status, r.read(), r.headers
        except urllib.error.HTTPError as e:
            if e.code != 429 and e.code < 500:
                return e.code, b"", e.headers
            err = e
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            err = e
        time.sleep(min(2 ** i, 60))
    raise RuntimeError(f"{path}: {err}")


def land(snap, path):
    """Fetch path into the snapshot; return its manifest row."""
    status, body, headers = fetch(path)
    row = {"url": SITE + path, "path": path.lstrip("/"), "status": status,
           "bytes": len(body) if status == 200 else None,
           "sha256": hashlib.sha256(body).hexdigest() if status == 200 else None,
           "compressed_bytes": None,
           "content_type": headers.get("Content-Type"), "last_modified": headers.get("Last-Modified"),
           "location": headers.get("Location"),
           "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}
    if status == 200:
        row["compressed_bytes"] = write_gz(snap / row["path"], body)
    return row


def write_gz(f, body):
    """<f>.gz holding body (mtime 0: the same bytes gzip the same); its size."""
    gz = f.with_name(f.name + ".gz")
    gz.parent.mkdir(parents=True, exist_ok=True)
    tmp = gz.with_name(gz.name + ".partial")
    tmp.write_bytes(z := gzip.compress(body, mtime=0))
    tmp.replace(gz)
    return len(z)


def read_gz(snap, path):
    return gzip.decompress((snap / (path + ".gz")).read_bytes())


def is_html(row):
    return row["status"] == 200 and (row["content_type"] or "").startswith("text/html")


def crawl(root, date):
    snap = root / "raw" / date
    snap.mkdir(parents=True, exist_ok=True)
    manifest = snap / "manifest.jsonl"
    done = {}
    if manifest.exists():
        for line in manifest.read_text().splitlines():
            r = json.loads(line)
            done["/" + r["path"]] = r
    t0, fetched, failed = time.time(), 0, []
    # Resume: paths already in the manifest are walked again from the landed HTML, so their
    # links are rediscovered; only paths not in it are fetched.
    todo = [canonical(t) for t in TREES] + SEEDS
    seen = set(todo)
    with manifest.open("a") as out, cf.ThreadPoolExecutor(WORKERS) as pool:
        running = {}
        while todo or running:
            while todo and len(running) < WORKERS * 4:
                p = todo.pop()
                if p in done:
                    r = done[p]
                    if is_html(r):
                        new = [q for q in links(p, read_gz(snap, r["path"])) if q not in seen]
                        seen.update(new)
                        todo += new
                    continue
                running[pool.submit(land, snap, p)] = p
            if not running:
                continue
            for fut in cf.wait(running, return_when=cf.FIRST_COMPLETED).done:
                p = running.pop(fut)
                try:
                    r = fut.result()
                except RuntimeError as e:
                    failed.append(str(e))
                    continue
                out.write(json.dumps(r) + "\n")
                out.flush()
                done[p] = r
                fetched += 1
                if is_html(r):
                    new = [q for q in links(p, read_gz(snap, r["path"])) if q not in seen]
                    seen.update(new)
                    todo += new
                if r["status"] in (301, 302, 307, 308) and r["location"]:
                    u = urllib.parse.urlsplit(urllib.parse.urljoin(SITE + p, r["location"]))
                    q = canonical(u.path) if u.netloc == "bioconductor.org" else None
                    if q and q not in seen:
                        seen.add(q)
                        todo.append(q)
                if fetched % 2000 == 0:
                    print(f"  {fetched:,d} fetched, {len(todo) + len(running):,d} queued  "
                          f"{time.time() - t0:.0f}s", flush=True)
    rows = list(done.values())
    ok = [r for r in rows if r["status"] == 200]
    by_status = {}
    for r in rows:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    print(f"snapshot {snap}: {len(ok):,d} files, {sum(r['bytes'] for r in ok):,d} bytes "
          f"({sum(r['compressed_bytes'] for r in ok):,d} gzipped); "
          f"statuses {by_status}; fetched {fetched:,d} this run in {time.time() - t0:.0f}s", flush=True)
    for t in TREES:
        tr = [r for r in ok if ("/" + r["path"]).startswith(t)]
        print(f"  {t}: {len(tr):,d} files, {sum(r['bytes'] for r in tr):,d} bytes", flush=True)
    if failed:
        print(f"{len(failed)} paths failed after {TRIES} tries (rerun to retry):", *failed[:20],
              sep="\n  ", file=sys.stderr)
        return 1


# --- 2. normalise ----------------------------------------------------------------------

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
# A .tab path -> kind; tree, category, package (dir), stem: the file name before _stats.tab.
# RE2 has no backreferences, so `stem` is checked against the package or prefix in SQL.
TAB_KINDS = """
    CASE WHEN regexp_matches(path, '^packages/[^/]+/[^/]+/[^/]+/[^/]+_stats\\.tab$') THEN 'package'
         WHEN regexp_matches(path, '^packages/[^/]+/[^/]+/[^/]+_pkg_stats\\.tab$') THEN 'pkg_stats'
         WHEN regexp_matches(path, '^packages/[^/]+/[^/]+/[^/]+_pkg_scores\\.tab$') THEN 'pkg_scores'
         WHEN regexp_matches(path, '^packages/[^/]+/[^/]+/[^/]+_stats\\.tab$') THEN 'category' END"""


def connect():
    import duckdb
    con = duckdb.connect()
    tmp = SCRATCH / "duckdb-published-stats"
    tmp.mkdir(parents=True, exist_ok=True)
    # ponytail: fixed caps for a shared, busy host.
    con.execute(f"SET temp_directory = '{tmp}'; SET memory_limit = '32GB'; SET threads = 16")
    return con


def load_tabs(con, snap):
    """tabs: one row per .tab in the manifest, classified; rows: every data row of the
    year and all-years files, with its file's line count for the no-drop check."""
    con.execute(f"""
        CREATE OR REPLACE TABLE tabs AS
        SELECT path, fetched_at::TIMESTAMPTZ AS fetched_at, {TAB_KINDS} AS kind,
               split_part(path, '/', 2) AS tree, split_part(path, '/', 3) AS category,
               CASE WHEN len(string_split(path, '/')) = 5 THEN split_part(path, '/', 4) END AS package,
               regexp_extract(path, '([^/]+)_stats\\.tab$', 1) AS stem
        FROM read_json('{snap}/manifest.jsonl', format = 'newline_delimited')
        WHERE status = 200 AND path LIKE '%.tab'""")
    bad = con.execute("SELECT path FROM tabs WHERE kind IS NULL").fetchall()
    if bad:
        raise SystemExit(f"{len(bad)} .tab files of no known kind, e.g. {bad[:5]}")
    # stem is <name>_<YYYY> or <name>; <name> is the package dir, or the category prefix.
    con.execute(f"""
        CREATE OR REPLACE TABLE tabs AS
        SELECT *, CASE WHEN regexp_matches(stem, '_[0-9]{{4}}$') THEN right(stem, 4)::INT END AS file_year,
               CASE WHEN regexp_matches(stem, '_[0-9]{{4}}$') THEN stem[:-6] ELSE stem END AS name
        FROM tabs""")
    prefixes = ", ".join(f"('{c}', '{p}')" for c, p in PREFIXES.items())
    bad = con.execute(f"""
        SELECT path FROM tabs LEFT JOIN (VALUES {prefixes}) v(category, prefix) USING (category)
        WHERE (kind = 'package' AND name <> package)
           OR (kind = 'category' AND name IS DISTINCT FROM prefix)""").fetchall()
    if bad:
        raise SystemExit(f"{len(bad)} .tab names not matching their directory, e.g. {bad[:5]}")

    paths = [p for (p,) in con.execute(
        "SELECT path FROM tabs WHERE kind IN ('package', 'category') ORDER BY path").fetchall()]
    n = len(snap.as_posix()) + 1
    con.execute(f"""
        CREATE OR REPLACE TABLE rows AS
        SELECT filename[{n + 1}:-4] AS path, Year AS year_text, Month AS month_text,
               Nb_of_distinct_IPs AS distinct_ips, Nb_of_downloads AS downloads
        FROM read_csv($files, delim = '\t', header = true, filename = true, quote = '',
                      columns = {{'Year': 'VARCHAR', 'Month': 'VARCHAR',
                                 'Nb_of_distinct_IPs': 'BIGINT', 'Nb_of_downloads': 'BIGINT'}})""",
                {"files": [f"{snap}/{p}.gz" for p in paths]})
    # No silent drops: data rows per file = its non-empty lines after the header, counted
    # here independently of DuckDB's CSV reader.
    parsed = dict(con.execute("SELECT path, count(*) FROM rows GROUP BY path").fetchall())
    short = [(p, k, parsed.get(p, 0)) for p in paths
             if (k := sum(1 for l in read_gz(snap, p).split(b"\n") if l.strip()) - 1) != parsed.get(p, 0)]
    if short:
        raise SystemExit(f"{len(short)} files with rows not parsed, e.g. {short[:5]}")
    months = ", ".join(f"'{m}'" for m in MONTHS)
    bad = con.execute(f"""
        SELECT path, year_text, month_text FROM rows
        WHERE NOT regexp_matches(year_text, '^[0-9]{{4}}$') OR month_text NOT IN ({months}, 'all')
           OR distinct_ips IS NULL OR downloads IS NULL""").fetchall()
    if bad:
        raise SystemExit(f"{len(bad)} rows with an unknown year or month, e.g. {bad[:5]}")
    con.execute(f"""
        CREATE OR REPLACE TABLE rows AS
        SELECT tabs.*, year_text::INT AS year,
               CASE month_text WHEN 'all' THEN NULL ELSE list_position([{months}], month_text) END AS month,
               distinct_ips, downloads
        FROM rows JOIN tabs USING (path)""")
    return len(paths)


def normalise(root, date):
    snap = root / "raw" / date
    con = connect()
    t0 = time.time()
    n_files = load_tabs(con, snap)
    print(f"snapshot {date}: parsed {n_files:,d} year and all-years .tab files, "
          f"{con.execute('SELECT count(*) FROM rows').fetchone()[0]:,d} rows, none dropped  "
          f"{time.time() - t0:.0f}s", flush=True)
    for kind, n in con.execute("SELECT kind, count(*) FROM tabs GROUP BY ALL ORDER BY ALL").fetchall():
        print(f"  {kind}: {n:,d} files")

    # The per-year files are the monthly rows; the all-years files must repeat them.
    checks = {
        "year file whose rows are not that year":
            "SELECT DISTINCT path FROM rows WHERE file_year IS NOT NULL AND year <> file_year",
        "year file without exactly Jan..Dec + all":
            """SELECT path FROM rows WHERE file_year IS NOT NULL GROUP BY path
               HAVING count(*) <> 13 OR count(DISTINCT coalesce(month, 0)) <> 13""",
        "all-years row differing from or missing in its year file":
            """SELECT tree, category, package, year, month, distinct_ips, downloads FROM rows WHERE file_year IS NULL
               EXCEPT ALL
               SELECT tree, category, package, year, month, distinct_ips, downloads FROM rows WHERE file_year IS NOT NULL""",
        "year-file row missing in the all-years file":
            """SELECT tree, category, package, year, month, distinct_ips, downloads FROM rows WHERE file_year IS NOT NULL
               EXCEPT ALL
               SELECT tree, category, package, year, month, distinct_ips, downloads FROM rows WHERE file_year IS NULL""",
    }
    for what, q in checks.items():
        n = con.execute(f"SELECT count(*) FROM ({q})").fetchone()[0]
        print(f"  check: {n:,d} {what}")

    pkg_files = [f"{snap}/{p}.gz" for (p,) in con.execute(
        "SELECT path FROM tabs WHERE kind = 'pkg_stats' ORDER BY path").fetchall()]
    if pkg_files:
        n = len(snap.as_posix()) + 1
        con.execute(f"""
            CREATE OR REPLACE TABLE pkg_stats AS
            SELECT split_part(filename[{n + 1}:], '/', 2) AS tree, split_part(filename[{n + 1}:], '/', 3) AS category,
                   Package AS package, Year AS year,
                   CASE Month WHEN 'all' THEN NULL ELSE list_position([{', '.join(repr(m) for m in MONTHS)}], Month) END AS month,
                   Nb_of_distinct_IPs AS distinct_ips, Nb_of_downloads AS downloads
            FROM read_csv($files, delim = '\t', header = true, filename = true, quote = '',
                          columns = {{'Package': 'VARCHAR', 'Year': 'INT', 'Month': 'VARCHAR',
                                     'Nb_of_distinct_IPs': 'BIGINT', 'Nb_of_downloads': 'BIGINT'}})""",
                    {"files": pkg_files})
        cols = "tree, category, package, year, month, distinct_ips, downloads"
        for what, a, b in (("<p>_pkg_stats.tab row not in a package's year file", "pkg_stats",
                            "rows WHERE package IS NOT NULL AND file_year IS NOT NULL"),
                           ("package year-file row not in <p>_pkg_stats.tab",
                            "rows WHERE package IS NOT NULL AND file_year IS NOT NULL", "pkg_stats")):
            n = con.execute(f"SELECT count(*) FROM (SELECT {cols} FROM {a} EXCEPT ALL SELECT {cols} FROM {b})").fetchone()[0]
            print(f"  check: {n:,d} {what}")

    # A (tree, category, package, year) with only an all-years file still counts.
    pick = """
        SELECT * FROM rows WHERE file_year IS NOT NULL
        UNION ALL
        SELECT * FROM rows r WHERE file_year IS NULL AND NOT EXISTS (
            SELECT 1 FROM tabs t WHERE t.file_year = r.year AND t.tree = r.tree AND t.category = r.category
                                   AND t.package IS NOT DISTINCT FROM r.package)"""
    keys = "tree, category, package, year"
    for name, cols, where in (("published_month", f"{keys}, month", "month IS NOT NULL"),
                              ("published_year", keys, "month IS NULL")):
        out = root / f"{name}.parquet"
        tmp = out.with_suffix(".parquet.partial")
        con.execute(f"""
            COPY (SELECT {cols}, distinct_ips, downloads, 'published' AS source, fetched_at
                  FROM ({pick}) WHERE {where} ORDER BY {cols} NULLS FIRST)
            TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)""")
        tmp.replace(out)
        print(f"{out}:")
        for r in con.execute(f"""SELECT tree, package IS NULL, count(*), count(DISTINCT category || '/' || package),
                                        min(year), max(year) FROM '{out}' GROUP BY ALL ORDER BY ALL""").fetchall():
            print(f"  {r[0]} {'category totals' if r[1] else 'packages'}: {r[2]:,d} rows"
                  f"{'' if r[1] else f', {r[3]:,d} packages'}, {r[4]}-{r[5]}")


# --- 3. compare ------------------------------------------------------------------------

WINDOW = ("2020-01-01", "2026-09-01")


def md(header, rows):
    def cell(v):
        return ("–" if v is None else f"{v:+.2f} %" if isinstance(v, float)
                else f"{v:,d}" if isinstance(v, int) else str(v))
    return "\n".join(["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
                     + ["| " + " | ".join(cell(v) for v in r) + " |" for r in rows]) + "\n"


def fl(v):
    return None if v is None else float(v)


def pct(a, b):
    return f"round(100.0 * ({a} - {b}) / nullif({b}, 0), 2)"


def compare(root, stats):
    """Markdown on stdout: published (tree 'stats') vs download-stats.py's fixed columns."""
    con = connect()
    w0, w1 = WINDOW
    q = lambda sql: con.execute(sql).fetchall()
    con.execute(f"""
        CREATE TABLE pub AS SELECT make_date(year, month, 1) AS month, category, package, distinct_ips, downloads
        FROM '{root}/published_month.parquet'
        WHERE tree = 'stats' AND make_date(year, month, 1) BETWEEN '{w0}' AND '{w1}';
        CREATE TABLE ours AS SELECT month, category, package, distinct_clients, downloads
        FROM '{stats}/package_month.parquet' WHERE month BETWEEN '{w0}' AND '{w1}'
        UNION ALL
        SELECT month, category, NULL, distinct_clients, downloads
        FROM '{stats}/category_month.parquet' WHERE month BETWEEN '{w0}' AND '{w1}';
        CREATE TABLE cmp AS
        SELECT coalesce(p.month, o.month) AS month, coalesce(p.category, o.category) AS category,
               coalesce(p.package, o.package) AS package, p.distinct_ips AS pub_ips, p.downloads AS pub_dl,
               coalesce(o.distinct_clients, 0) AS our_ips, coalesce(o.downloads, 0) AS our_dl
        FROM pub p FULL JOIN ours o ON p.month = o.month AND p.category = o.category
                                    AND p.package IS NOT DISTINCT FROM o.package
        WHERE coalesce(p.downloads, 0) > 0 OR coalesce(o.downloads, 0) > 0;
        UPDATE cmp SET pub_ips = coalesce(pub_ips, 0), pub_dl = coalesce(pub_dl, 0);""")
    print(f"# Published /packages/stats/ vs our fixed-definition columns, {w0[:7]} → {w1[:7]}\n")
    print("Published: tree `stats` of the snapshot (`published_month.parquet`). Ours: "
          "`package_month` / `category_month` `downloads`, `distinct_clients` (download-stats.py). "
          "Δ = (ours − published) / published.\n")

    print("## Coverage\n")
    rows = q("""SELECT package IS NULL, count(*), count(*) FILTER (pub_dl > 0 AND our_dl > 0),
                       count(*) FILTER (our_dl = 0), sum(pub_dl) FILTER (our_dl = 0)::BIGINT,
                       count(*) FILTER (pub_dl = 0), sum(our_dl) FILTER (pub_dl = 0)::BIGINT
                FROM cmp GROUP BY 1 ORDER BY 1""")
    print(md(["rows", "months with downloads", "in both", "published only", "their downloads",
              "ours only", "their downloads"],
             [("categories" if r[0] else "package", *r[1:]) for r in rows]))

    print("## Distribution of Δ, package-months in both with ≥ 100 published downloads\n")
    qs = [1, 5, 25, 50, 75, 95, 99]
    rows = []
    for what, a, b in (("downloads", "our_dl", "pub_dl"), ("distinct IPs", "our_ips", "pub_ips")):
        for cat in ("all", "bioc", "data-annotation", "data-experiment", "workflows"):
            where = "" if cat == "all" else f"AND category = '{cat}'"
            r = q(f"""SELECT count(*), {', '.join(f'quantile_cont(d, {p / 100})' for p in qs)},
                             avg((abs(d) <= 1)::INT) * 100, avg((abs(d) <= 5)::INT) * 100
                      FROM (SELECT {pct(a, b)} AS d FROM cmp
                            WHERE package IS NOT NULL AND pub_dl >= 100 AND our_dl > 0 {where})""")[0]
            if r[0]:
                rows.append((what, cat, r[0], *map(float, r[1:8]), f"{r[8]:.1f} %", f"{r[9]:.1f} %"))
    print(md(["measure", "category", "n", *[f"p{p}" for p in qs], "within ±1 %", "within ±5 %"], rows))

    print("## Category totals by month, where |Δ| > 2 %\n")
    rows = q(f"""SELECT strftime(month, '%Y-%m'), category, pub_dl, our_dl, {pct('our_dl', 'pub_dl')},
                        pub_ips, our_ips, {pct('our_ips', 'pub_ips')}
                 FROM cmp WHERE package IS NULL
                   AND (abs({pct('our_dl', 'pub_dl')}) > 2 OR abs({pct('our_ips', 'pub_ips')}) > 2)
                 ORDER BY month, category""")
    n = q("SELECT count(*) FROM cmp WHERE package IS NULL")[0][0]
    print(f"{len(rows)} of {n} category-months.\n")
    print(md(["month", "category", "pub downloads", "our downloads", "Δ", "pub IPs", "our IPs", "Δ"],
             [(*r[:4], fl(r[4]), r[5], r[6], fl(r[7])) for r in rows]))

    print("## Outlier package-months (|Δ downloads| > 5 %, ≥ 1,000 published downloads), by month\n")
    rows = q(f"""SELECT strftime(month, '%Y-%m'), count(*), count(*) FILTER (our_dl > pub_dl),
                        count(*) FILTER (abs({pct('our_ips', 'pub_ips')}) > 5),
                        (SELECT count(*) FROM cmp c WHERE c.month = cmp.month AND c.package IS NOT NULL
                                                   AND c.pub_dl >= 1000)
                 FROM cmp WHERE package IS NOT NULL AND pub_dl >= 1000
                   AND abs({pct('our_dl', 'pub_dl')}) > 5 GROUP BY month ORDER BY month""")
    print(md(["month", "outliers", "ours higher", "of which |Δ IPs| > 5 %", "package-months ≥ 1,000"], rows))
    print("Largest 25 by |Δ downloads|:\n")
    rows = q(f"""SELECT strftime(month, '%Y-%m'), category, package, pub_dl, our_dl, {pct('our_dl', 'pub_dl')},
                        pub_ips, our_ips, {pct('our_ips', 'pub_ips')}
                 FROM cmp WHERE package IS NOT NULL AND pub_dl >= 1000
                 ORDER BY abs(our_dl - pub_dl) DESC LIMIT 25""")
    print(md(["month", "category", "package", "pub downloads", "our downloads", "Δ", "pub IPs", "our IPs", "Δ"],
             [(*r[:5], fl(r[5]), r[6], r[7], fl(r[8])) for r in rows]))

    day_runs(con, stats, q)
    head_requests(con, q)
    cross_category(con, stats, q)


def universe(con, stats):
    con.execute(f"""
        CREATE OR REPLACE TABLE universe AS
        SELECT DISTINCT regexp_extract(filename, '-((bioc|data-annotation|data-experiment|workflows))\\.txt$', 1)
                        AS category, unnest(string_split(content, chr(10))) AS package
        FROM read_text('{stats}/packages-index/*.txt')""")


def day_runs(con, stats, q):
    """Is a month that differs the published source missing whole log days, or counting
    some twice? One run of days [a, b] per month, s = -1 (missing) or +1 (counted twice),
    fit to the published category totals, downloads and distinct IPs together. Missing
    days lower both (a client stays if it has a download outside the run); days counted
    twice raise downloads and leave distinct IPs alone. The fitted run is then tested,
    out of sample, on the top 20 bioc packages."""
    universe(con, stats)
    months = [r[0] for r in q(f"""
        SELECT DISTINCT month FROM cmp WHERE package IS NULL AND pub_dl > 0
          AND (abs({pct('our_dl', 'pub_dl')}) > 2 OR abs({pct('our_ips', 'pub_ips')}) > 2) ORDER BY month""")]
    print("## Hypothesis: the published source is missing log days, or counts some twice\n")
    print("Months where a category total differs by more than 2 %: "
          f"{', '.join(m.strftime('%Y-%m') for m in months) or 'none'}. For each, the run of whole "
          "UTC days whose removal (missing) or repetition (counted twice) best fits the published "
          "category totals, downloads and distinct IPs together; then the same run applied to "
          "the top 20 bioc packages, which the fit did not see.\n")
    rows = []
    for m in months:
        con.execute(f"""
            CREATE OR REPLACE TABLE days AS
            SELECT day(date) AS d, category, package, client_id, downloads
            FROM read_parquet('{stats}/clients/year={m.year}/month={m.month}/clients.parquet')
            SEMI JOIN universe USING (category, package)""")
        a, b, sign, _ = q(f"""
            WITH runs AS (SELECT a, b, s, ((1::BIGINT << b) - (1::BIGINT << (a - 1))) AS g
                          FROM range(1, 32) r(a), range(1, 32) t(b), (VALUES (-1), (1)) u(s) WHERE a <= b
                          UNION ALL SELECT 0, -1, 0, 0),
            d AS (SELECT d, category, sum(downloads) AS n FROM days GROUP BY ALL),
            c AS (SELECT category, bit_or(1::BIGINT << (d - 1)) AS mask FROM days GROUP BY category, client_id),
            dl AS (SELECT a, b, s, category, sum(n) + s * coalesce(sum(n) FILTER (d BETWEEN a AND b), 0) AS n
                   FROM runs, d GROUP BY a, b, s, category),
            ips AS (SELECT a, b, s, category, count(*) FILTER (s >= 0 OR mask & ~g <> 0) AS n
                    FROM runs, c GROUP BY a, b, s, category)
            SELECT a, b, s, sum(abs(dl.n - p.pub_dl) / p.pub_dl + abs(ips.n - p.pub_ips) / p.pub_ips) AS err
            FROM dl JOIN ips USING (a, b, s, category)
            JOIN cmp p ON p.month = DATE '{m}' AND p.package IS NULL AND p.category = dl.category
            WHERE p.pub_dl > 0
            GROUP BY a, b, s ORDER BY err LIMIT 1""")[0]
        run = ("none" if sign == 0 else
               f"{m:%m}-{a:02d} → {b:02d} {'missing' if sign < 0 else 'counted twice'}")
        fitted = f"""SELECT category, package, sum(downloads) + {sign} * coalesce(sum(downloads) FILTER
                                                (d BETWEEN {a} AND {b}), 0) AS dl,
                            count(DISTINCT client_id) FILTER ({sign} >= 0 OR NOT d BETWEEN {a} AND {b}) AS ips
                     FROM days"""
        for r in q(f"""
            WITH k AS ({fitted} GROUP BY GROUPING SETS ((category), (category, package)))
            SELECT category, pub_dl, our_dl, k.dl, {pct('k.dl', 'pub_dl')}, pub_ips, our_ips, k.ips,
                   {pct('k.ips', 'pub_ips')}
            FROM k JOIN cmp USING (category) WHERE month = DATE '{m}' AND cmp.package IS NULL AND k.package IS NULL
              AND pub_dl > 0
            ORDER BY category"""):
            rows.append((f"{m:%Y-%m}", run, *r[:4], fl(r[4]), *r[5:8], fl(r[8])))
        r = q(f"""
            WITH k AS ({fitted} WHERE category = 'bioc' GROUP BY category, package),
            top AS (SELECT * FROM cmp WHERE month = DATE '{m}' AND category = 'bioc' AND package IS NOT NULL
                    ORDER BY pub_dl DESC LIMIT 20)
            SELECT median(abs({pct('k.dl', 'pub_dl')})), median(abs({pct('k.ips', 'pub_ips')})),
                   median(abs({pct('our_dl', 'pub_dl')})), median(abs({pct('our_ips', 'pub_ips')}))
            FROM top JOIN k USING (category, package)""")[0]
        rows.append((f"{m:%Y-%m}", run, "top 20 bioc packages, median \\|Δ\\|", "", f"{r[2]:.2f} %", "",
                     f"{r[0]:.2f} %", "", f"{r[3]:.2f} %", "", f"{r[1]:.2f} %"))
    print(md(["month", "best-fit run of days", "category", "pub downloads", "ours", "ours, fitted", "Δ",
              "pub IPs", "ours", "ours, fitted", "Δ"], rows))
    print("Top-20 rows: the Δ columns are median |Δ| before (\"ours\") and after (\"ours, fitted\").\n")


def head_requests(con, q):
    """Do the published files count HEAD requests? Our fixed definition excludes them.
    HEAD with status 200 per (month, category, package), from the downloads view with
    only the method and status swapped, so URL parsing and hosts are the same."""
    view = (HERE / "sql" / "downloads.sql").read_text()
    swaps = {"cs_method <> 'HEAD'": "cs_method = 'HEAD'",
             "sc_status IN ('200','301','302','307','308')": "sc_status = '200'",
             "VIEW downloads AS": "VIEW head_requests AS"}
    for a, b in swaps.items():
        assert view.count(a) == 1, f"sql/downloads.sql no longer has {a!r}"
        view = view.replace(a, b)
    for f in ("access.sql", "client_class.sql"):
        con.execute((HERE / "sql" / f).read_text())
    con.execute(view)
    t0 = time.time()
    con.execute(f"""
        CREATE OR REPLACE TABLE head AS
        SELECT make_date(year, month, 1) AS month, category, package, count(*) AS h200
        FROM head_requests SEMI JOIN universe USING (category, package)
        WHERE production AND make_date(year, month, 1) BETWEEN '{WINDOW[0]}' AND '{WINDOW[1]}'
        GROUP BY ALL""")
    con.execute("""
        CREATE OR REPLACE TABLE cmp_head AS
        SELECT c.*, coalesce(h.h200, 0) AS h200, strftime(month, '%Y-') || (CASE WHEN month(month) <= 6
               THEN 'H1' ELSE 'H2' END) AS half
        FROM cmp c LEFT JOIN (FROM head UNION ALL
                              SELECT month, category, NULL, sum(h200) FROM head GROUP BY ALL) h
          ON h.month = c.month AND h.category = c.category AND h.package IS NOT DISTINCT FROM c.package""")
    print("## Hypothesis: the published files count HEAD requests\n")
    print(f"HEAD requests answered 200 (package paths, production hosts; {time.time() - t0:.0f} s to "
          "count) added to ours. Δ as above, then \"+HEAD\" = (ours + HEAD 200 − published) / "
          "published. By half-year, median over months (bioc total) and over package-months with "
          "≥ 100 published downloads (all categories):\n")
    rows = q(f"""
        SELECT half,
               median({pct('our_dl', 'pub_dl')}) FILTER (package IS NULL AND category = 'bioc'),
               median({pct('our_dl + h200', 'pub_dl')}) FILTER (package IS NULL AND category = 'bioc'),
               count(*) FILTER (package IS NOT NULL AND pub_dl >= 100 AND our_dl > 0),
               median(abs({pct('our_dl', 'pub_dl')})) FILTER (package IS NOT NULL AND pub_dl >= 100 AND our_dl > 0),
               median(abs({pct('our_dl + h200', 'pub_dl')})) FILTER (package IS NOT NULL AND pub_dl >= 100 AND our_dl > 0)
        FROM cmp_head GROUP BY half ORDER BY half""")
    print(md(["half-year", "bioc total, median Δ", "+HEAD", "package-months", "median |Δ|", "+HEAD"],
             [(r[0], fl(r[1]), fl(r[2]), r[3], f"{r[4]:.2f} %", f"{r[5]:.2f} %") for r in rows]))
    print("Category-months with |Δ downloads| > 2 %:\n")
    rows = q(f"""
        SELECT strftime(month, '%Y-%m'), category, pub_dl, our_dl, h200, {pct('our_dl', 'pub_dl')},
               {pct('our_dl + h200', 'pub_dl')}, {pct('our_ips', 'pub_ips')}
        FROM cmp_head WHERE package IS NULL AND pub_dl > 0 AND abs({pct('our_dl', 'pub_dl')}) > 2
        ORDER BY month, category""")
    print(md(["month", "category", "pub downloads", "ours", "HEAD 200", "Δ", "+HEAD", "Δ IPs"],
             [(*r[:5], fl(r[5]), fl(r[6]), fl(r[7])) for r in rows]))


def cross_category(con, stats, q):
    """Does the published file for (category, package) count the package's downloads
    under every category path? Only package-months with material cross-category traffic
    (all-paths downloads > own-path by > 2 %) can tell the rules apart. Each is assigned
    the rule closest to the published downloads, if within 2 %: own path or all paths,
    each with or without HEAD 200s."""
    con.execute(f"""
        CREATE OR REPLACE TABLE anycat AS
        SELECT make_date(year, month, 1) AS month, package, sum(downloads) AS dl
        FROM read_parquet('{stats}/clients/*/*/clients.parquet', hive_partitioning = true)
        WHERE make_date(year, month, 1) BETWEEN '{WINDOW[0]}' AND '{WINDOW[1]}'
        GROUP BY ALL""")
    con.execute("""
        CREATE OR REPLACE TABLE xcat AS
        WITH h AS (SELECT month, package, sum(h200) AS h200 FROM head GROUP BY ALL),
        x AS (SELECT c.*, a.dl AS any_dl, coalesce(h.h200, 0) AS any_h200
              FROM cmp_head c JOIN anycat a USING (month, package) LEFT JOIN h USING (month, package)
              WHERE c.package IS NOT NULL AND pub_dl >= 100 AND a.dl > 1.02 * our_dl),
        r AS (SELECT x.*, rule, abs(n - pub_dl) / pub_dl AS err
              FROM x, LATERAL (VALUES ('own path', our_dl), ('own path + HEAD', our_dl + h200),
                                      ('all paths', any_dl), ('all paths + HEAD', any_dl + any_h200)) v(rule, n))
        SELECT month, category, package, CASE WHEN min(err) <= 0.02 THEN arg_min(rule, err) ELSE 'neither' END AS rule
        FROM r GROUP BY ALL""")
    rules = ["own path", "own path + HEAD", "all paths", "all paths + HEAD", "neither"]
    counts = ", ".join(f"count(*) FILTER (rule = '{r}')" for r in rules)
    print("## Hypothesis: the published files count a package under every category path\n")
    print("Package-months with ≥ 100 published downloads whose downloads over all category "
          "paths exceed the own-path count by > 2 %, by the rule closest to the published "
          "downloads (within 2 %). By year and category:\n")
    rows = q(f"SELECT year(month)::VARCHAR, category, count(*), {counts} FROM xcat GROUP BY ALL ORDER BY ALL")
    print(md(["year", "category", "package-months", *rules], rows))
    print("Months where an all-paths rule fits most often:\n")
    rows = q(f"""SELECT strftime(month, '%Y-%m'), count(*), {counts} FROM xcat GROUP BY month
                 HAVING count(*) FILTER (rule LIKE 'all paths%') > 0
                 ORDER BY count(*) FILTER (rule LIKE 'all paths%') DESC, month LIMIT 15""")
    print(md(["month", "package-months", *rules], rows))


def latest(root):
    snaps = sorted(p.name for p in (root / "raw").iterdir() if re.fullmatch(r"\d{4}-\d{2}-\d{2}", p.name))
    if not snaps:
        raise SystemExit(f"no snapshot under {root}/raw; run crawl first")
    return snaps[-1]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("step", nargs="?", choices=["crawl", "normalise", "compare"])
    ap.add_argument("--root", default=ROOT, type=pathlib.Path, help=f"default {ROOT}")
    ap.add_argument("--date", help="snapshot date (crawl: default today UTC; else the latest)")
    ap.add_argument("--stats", default=STATS, type=pathlib.Path, help=f"download-stats.py --out, default {STATS}")
    ap.add_argument("--self-check", action="store_true")
    a = ap.parse_args()
    if a.self_check:
        return self_check()
    if a.step == "crawl":
        return crawl(a.root, a.date or dt.datetime.now(dt.timezone.utc).date().isoformat())
    if a.step == "normalise":
        return normalise(a.root, a.date or latest(a.root))
    if a.step == "compare":
        return compare(a.root, a.stats)
    ap.error("a step is required")


def self_check():
    page = b'<a HREF="bioc/limma/">x</a><img SRC="limma_2014_stats.png"><a href="../../main.css">' \
           b'<a href="https://github.com/x"><a href="/packages/devel/bioc/html/limma.html"><a href="a.tab#x">'
    got = links("/packages/oldstats/bioc/limma/index.html", page)
    assert got == {"/packages/oldstats/bioc/limma/bioc/limma/index.html",
                   "/packages/oldstats/bioc/limma/limma_2014_stats.png", "/packages/oldstats/main.css",
                   "/packages/oldstats/bioc/limma/a.tab"}, got
    assert canonical("/packages/stats/") == "/packages/stats/index.html"
    assert canonical("/packages/release/bioc/") is None
    print("self-check OK")


if __name__ == "__main__":
    sys.exit(main())
