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
   `source` is 'logs' for these. package_month, category_month and overall_month also
   carry the months before JOIN (2020-01, where the logs start) from the published
   /packages/stats/ snapshot (published-stats-mirror.py, #24) as source = 'published':
   `downloads`, and `distinct_clients` = its distinct IPs, NULL in overall_month (they
   do not add across categories); every other column NULL. Without --published's file
   they are left out.

3. stats/ — the /packages/stats/ tree (ADR 0004 contract) from the fixed columns:
     <cat>/<pkg>/<pkg>_stats.tab, <cat>/<pkg>/<pkg>_<year>_stats.tab
     <cat>/<cat>_stats.tab, <cat>/<cat>_<year>_stats.tab  (live names: data-annotation_stats.tab)
     <cat>/<p>_pkg_stats.tab, <cat>/<p>_pkg_scores.tab    (<p>: bioc, annotation, experiment, workflows)
   Formats match the live files: tab-separated, no trailing newline, a year block is its
   12 months plus an 'all' row whose distinct count is over the whole year (not a sum),
   years newest first, a package's years without downloads omitted, packages sorted
   case-insensitively. Download_score = floor(sum of monthly distinct clients over the
   12 complete months before the last data month / 12), for packages with any.
   Years from 2020 come from the logs. Earlier years cannot be regenerated, so they are
   the published snapshot's rows as published (--published and the published_year.parquet
   beside it, tree 'stats'); without those files the tree starts at 2020.

4. HTML pages (#28), generated from the stats/ tree itself, so they cannot disagree with the
   .tab files: stats/index.html, stats/<cat>.html (listing pages, the index's links),
   stats/<cat>/index.html, stats/<cat>/<pkg>/index.html and stats/static/barchart.js
   (copied from static/). Master's design as it was (#28 decision), Chart.js pinned.

Stages 2 and 3 count only (category, package) pairs a Bioconductor repository has listed:
the Package: fields of /packages/<v>/<cat>/src/contrib/PACKAGES for every release. The
CloudFront era answers any .../<cat>/src/contrib/<x>_<v>.tar.gz with a 302, so without
this, workflows alone gains ~1,800 names a month. The published tree applies the same
universe (it covers all 6,356 published pairs). The indexes are cached in
<out>/packages-index/; release and devel are refetched every run.
"""

import argparse, datetime as dt, functools, json, os, pathlib, re, runpy, shutil, sys, tempfile, time, urllib.error, urllib.request

HERE = pathlib.Path(__file__).resolve().parent
OUT = pathlib.Path("/data/davsean/bioc-traffic-stats")
SCRATCH = pathlib.Path("/data/davsean/tmp")   # /tmp is small on onclappc02
PUBLISHED = pathlib.Path("/data/davsean/bioc-published-stats/published_month.parquet")
JOIN = "2020-01-01"   # the first month of the logs; published numbers before it
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
# name -> its published rows before JOIN, from the published_history view.
HISTORY = {
    "package_month": "SELECT month, category, package, downloads, distinct_ips AS distinct_clients "
                     "FROM published_history WHERE package IS NOT NULL",
    "category_month": "SELECT month, category, downloads, distinct_ips AS distinct_clients "
                      "FROM published_history WHERE package IS NULL",
    "overall_month": "SELECT month, sum(downloads)::BIGINT AS downloads, NULL::BIGINT AS distinct_clients "
                     "FROM published_history WHERE package IS NULL GROUP BY month",
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


def write_aggregates(con, out, published=None):
    history = published is not None and published.exists()
    if history:
        con.execute(f"""
            CREATE OR REPLACE VIEW published_history AS
            SELECT make_date(year, month, 1) AS month, category, package, downloads, distinct_ips
            FROM read_parquet('{published}')
            WHERE tree = 'stats' AND downloads > 0 AND make_date(year, month, 1) < DATE '{JOIN}'""")
    elif published:
        print(f"  no {published}: aggregates start at {JOIN}", flush=True)
    for name, (keys, where) in AGGREGATES.items():
        t0 = time.time()
        query = (f"SELECT {keys},\n       {MEASURES},\n       'logs' AS source\n"
                 f"FROM repo_downloads WHERE {where}\nGROUP BY {keys}")
        if name in HISTORY and history:
            query += f"\nUNION ALL BY NAME\nSELECT *, 'published' AS source FROM ({HISTORY[name]})"
        replace_file(con, f"FROM ({query}) ORDER BY {keys}", out / f"{name}.parquet")
        n, h = con.execute(f"SELECT count(*), count(*) FILTER (source = 'published') "
                           f"FROM '{out / name}.parquet'").fetchone()
        print(f"  {name}: {n:,d} rows ({h:,d} published)  {time.time() - t0:.0f}s", flush=True)


# --- stage 3: the /packages/stats/ tree ------------------------------------------------

def year_block(year, months, total):
    """13 rows: 12 months (0 where none) and 'all'. months: {month: (ips, n)}."""
    rows = [(str(year), MONTHS[m - 1], *months.get(m, (0, 0))) for m in range(1, 13)]
    return rows + [(str(year), "all", *total)]


def tab(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join("\t".join(map(str, r)) for r in [header, *rows]))


def write_stats(con, out, published=None):
    """Rebuild <out>/stats beside the old tree, then swap it in."""
    t0 = time.time()
    as_of = con.execute("SELECT max(date) FROM repo_downloads").fetchone()[0]
    # Everything the tree needs in one scan: per package and per category, per month and year.
    rows = con.execute(f"""
        SELECT category, package, year, month(month), count(DISTINCT client_id), sum(downloads)::BIGINT
        FROM repo_downloads WHERE regexp_matches(package, '{PACKAGE_NAME}')
        GROUP BY GROUPING SETS ((category, package, year, month), (category, package, year),
                                (category, year, month), (category, year))""").fetchall()
    # The years before JOIN, as published: per package (or category, package NULL), months
    # and the year's 'all' row, whose distinct count does not come from the months.
    # Asked for and absent is fatal, not a skip: the upload would overwrite R2's full-history
    # files with ones starting at 2020 (bioc-infrastructure#114). None (the self-check) skips.
    if published:
        pub_year = published.with_name("published_year.parquet")
        if not (published.exists() and pub_year.exists()):
            raise SystemExit(f"no {published} or {pub_year}: refusing to write a tree without "
                             f"the years before {JOIN[:4]} (restore them, see published-normalise)")
        rows += con.execute(f"""
            SELECT category, package, year, month, distinct_ips, downloads FROM read_parquet('{published}')
            WHERE tree = 'stats' AND year < {JOIN[:4]}
              AND (package IS NULL OR regexp_matches(package, '{PACKAGE_NAME}'))
            UNION ALL
            SELECT category, package, year, NULL, distinct_ips, downloads FROM read_parquet('{pub_year}')
            WHERE tree = 'stats' AND year < {JOIN[:4]}
              AND (package IS NULL OR regexp_matches(package, '{PACKAGE_NAME}'))""").fetchall()
    months, years = {}, {}   # (category, package|None, year) -> {month: (ips, n)} / (ips, n)
    for cat, pkg, y, m, ips, n in rows:
        if m is None:
            years[cat, pkg, y] = (ips, n)
        else:
            months.setdefault((cat, pkg, y), {})[m] = (ips, n)

    # The 12 complete months before as_of's month, as (year, month).
    now = as_of.year * 12 + as_of.month - 1
    window = [(i // 12, i % 12 + 1) for i in range(now - 12, now)]
    new = out / "stats.partial"
    shutil.rmtree(new, ignore_errors=True)
    head = ("Year", "Month", "Nb_of_distinct_IPs", "Nb_of_downloads")
    for cat, (prefix, _) in CATEGORIES.items():
        d = new / cat
        # A category's years run back to its own first (workflows: 2013), as published.
        first = min((y for c, _, y in [*years, *months] if c == cat), default=as_of.year)
        all_years = []
        for y in range(as_of.year, first - 1, -1):
            block = year_block(y, months.get((cat, None, y), {}), years.get((cat, None, y), (0, 0)))
            tab(d / f"{cat}_{y}_stats.tab", head, block)
            all_years += block
        tab(d / f"{cat}_stats.tab", head, all_years)
        pkgs = sorted({p for c, p, _ in years if c == cat and p is not None}, key=lambda p: (p.lower(), p))
        pkg_rows, scores = [], []
        for p in pkgs:
            blocks = []
            for y in range(as_of.year, first - 1, -1):
                if (cat, p, y) not in years:
                    continue
                block = year_block(y, months.get((cat, p, y), {}), years[cat, p, y])
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
    write_pages(new, out / "packages-index")   # before the swap: pages and .tab files land together
    old = out / "stats.old"
    shutil.rmtree(old, ignore_errors=True)
    if (out / "stats").exists():
        (out / "stats").rename(old)
    new.rename(out / "stats")
    shutil.rmtree(old, ignore_errors=True)
    print(f"  stats/: {n_files:,d} files, data as of {as_of}  {time.time() - t0:.0f}s", flush=True)


# --- stage 4: the HTML pages -----------------------------------------------------------

CHARTJS = "https://cdn.jsdelivr.net/npm/chart.js@4.5.1"   # master loads the unpinned `chart.js`
TOP = {"bioc": 75, "data-annotation": 30, "data-experiment": 15}   # "Top N" on the listing pages
# category -> (listing page, plural in titles, singular in the "All ... packages" text, "software" etc.)
KIND = {"bioc": ("index.html", "software", "software"), "data-annotation": ("data-annotation.html", "annotation", "annotation"),
        "data-experiment": ("data-experiment.html", "experiment", "experiment"),
        "workflows": ("workflows.html", "workflow", "workflow")}
CSS = """        BODY {
            font-family: sans-serif;
            font-size: 10pt;
        }

        H1 {
            font-size: 16pt;
        }

        H2 {
            font-size: 14pt;
            margin-top: 8px;
            margin-bottom: 4px;
        }

        H3 {
            font-size: 12pt;
            margin-top: 0px;
            margin-bottom: 4px;
        }

        H4 {
            font-size: 11pt;
            margin-top: 0px;
            margin-bottom: 4px;
        }

        TABLE.stats {
            border: solid black 1px;
        }
"""
CSS_LISTING = CSS + """        .column-container {
            display: flex;
        }
        .column {
            flex: 1;
        }
"""
CANVAS_FALLBACK = "Your browser may be too old as it does not support the html canvas element."


def read_tab(path):
    """A .tab file as a list of rows (header dropped)."""
    return [l.split("\t") for l in path.read_text().split("\n")[1:] if l]


def year_blocks(path):
    """{year: [(month, ips, n) x 13]}, newest year first, from a *_stats.tab."""
    blocks = {}
    for y, m, ips, n in read_tab(path):
        blocks.setdefault(int(y), []).append((m, int(ips), int(n)))
    return dict(sorted(blocks.items(), reverse=True))


def stats_page(title, back, intro, blocks, prefix, as_of):
    """One package or category page. Master's markup, with its quirks (`algin`, the table layout)."""
    years = list(blocks)
    h = [f"""<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>

<head>
    <title> {title} </title>
    <style>
{CSS}    </style>
</head>

<body>
    <table style="width: 100%; border-spacing: 0px; border-collapse: collapse">
        <tr>
            <td style="padding: 0px; text-align: left">
                <i><a href="{back[0]}">Back to the "{back[1]}"</a></i>
            </td>
        </tr>
    </table>

    <h1 style="text-align: center"> {title} </h1>
    <p style="text-align: center">
        <i>Data as of {as_of}</i>
    </p>
    {intro}
    <p style="text-align: center">
        Number of package downloads from the Bioconductor software package
        repository, year by year, from {years[0]} back to {years[-1]} (years with no downloads are omitted):
    </p>
"""]
    for y, rows in blocks.items():
        h.append(f"""
        <hr />
        <h2 style="text-align: center">{y}</h2>
        <table width="90%" align="center">
            <tr>
                <td style="text-align: center">
                    <canvas id="barchart_{y}" class="barchart" width="720" height="400">
                        {CANVAS_FALLBACK}
                    </canvas>
                </td>
                <td style="text-align: center">
                    <table class="stats" algin="center">
                        <thead>
                            <tr>
                                <th>Month</th>
                                <th>Nb of distinct IPs</th>
                                <th>Nb of downloads</th>
                            </tr>
                        </thead>
                        <tbody>
""")
        for m, ips, n in rows:
            h.append(f"""                            <tr>
                                <td style="text-align: right">{m}/{y}</td>
                                <td style="text-align: right; background: #aaaaff">{ips}</td>
                                <td style="text-align: right; background: #ddddff">{n}</td>
                            </tr>
""")
        h.append(f"""                        </tbody>
                    </table>
                    <a href="{prefix}_{y}_stats.tab">{prefix}_{y}_stats.tab</a>
                </td>
            </tr>
        </table>
""")
    data = {str(y): [{"downloads": n, "month": m, "unique_ips": ips, "year": y} for m, ips, n in rows]
            for y, rows in sorted(blocks.items())}
    h.append(f"""
    <p style="text-align: center">
        All years in one file:&nbsp;<a href="{prefix}_stats.tab">{prefix}_stats.tab</a>
    </p>
    <script src="{CHARTJS}"></script>
    <script type="text/javascript">
        var data_table = {json.dumps(data)};
    </script>
    <script type="module" src="/packages/stats/static/barchart.js"></script>
</body>

</html>
""")
    return "".join(h)


def listing_page(cat, scores, as_of):
    """index.html / data-annotation.html / ...: the top packages, then every package by letter."""
    page, kind, _ = KIND[cat]
    prefix = CATEGORIES[cat][0]
    nav = "".join(f'<I><A HREF="{KIND[c][0]}">Bioconductor {KIND[c][1]} packages</A></I>&nbsp;&nbsp;&nbsp;&nbsp;\n                '
                  for c in CATEGORIES if c != cat)
    title = f"Download stats for Bioconductor {kind} packages"
    h = [f"""<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<HTML>

<HEAD>
    <TITLE>{title}</TITLE>
    <style>
{CSS_LISTING}    </style>
</HEAD>

<BODY>
    <TABLE style="width: 100%; border-spacing: 0px; border-collapse: collapse;">
        <TR>
            <TD style="padding: 0px; text-align: right;">
                <I>See download stats for:</I>&nbsp;&nbsp;&nbsp;&nbsp;
                {nav}
            </TD>
        </TR>
    </TABLE>
    <H1 style="text-align: center;">{title}</H1>
    <p>  </p>
    <P style="text-align: center;">
        <I>Data as of {as_of}.</I>
    </P>
    <P>The number reported next to each package name is the <I>download score</I>, that is, the average number of
        distinct IPs that "hit" the package each month for the last 12 months (not counting the current month).</P>
"""]
    top = sorted(scores.items(), key=lambda kv: (-kv[1], kv[0].lower(), kv[0]))[:TOP.get(cat, 0)]
    if top:
        rows = -(-len(top) // 3)
        h.append(f"""    <HR>
    <H2>Top {TOP[cat]}</H2>
    <table class="pkg_index">
        <tbody>
            <tr>
                <td style="vertical-align: top; width:300px;">
""")
        for r in range(rows):
            h.append('                <tr class="pkg_index">\n')
            for rank in range(r, len(top), rows):
                p, score = top[rank]
                h.append(f"""                         <td
                            style="width:25px; text-align: right">{rank + 1}</td>
                        <td>
                            <a href="{cat}/{p}/">
                                {p}&nbsp;({score})
                            </a>
                        </td>
""")
            h.append("                    </tr>\n")
        h.append("                </td>\n            </tr>\n                </td>\n            </tr>\n        </tbody>\n    </table>\n")
    if top:   # workflows has no Top N, and master's page has no "All" heading either
        h.append(f"    <hr>\n    <h2>All {kind} packages</h2>\n")
    h.append(f"""    <p>
        All {kind} package stats in one file:&nbsp;
        <a href="{cat}/{prefix}_pkg_stats.tab">{prefix}_pkg_stats.tab</a>
    </p>
    <p>
        All {kind} download scores in one file:&nbsp;
        <a href="{cat}/{prefix}_pkg_scores.tab">{prefix}_pkg_scores.tab</a>
    </p>
    <p style="text-align: center">
        <a href="{cat}/index.html">
            See Download stats for Bioconductor {kind} repository (all packages combined)
        </a>
    </p>
""")
    letters = {}
    for p in sorted(scores, key=lambda p: (p.lower(), p)):
        letters.setdefault(p[0].upper(), []).append(p)
    for L, pkgs in letters.items():
        k = -(-len(pkgs) // 3)
        h.append(f'    <h3 style="font-family: monospace; font-size: larger;">{L}</h3>\n    <div class="column-container">\n')
        for c in range(3):
            h.append('        <div class="column">\n')
            h += [f'            <p>\n                <a href="{cat}/{p}/">{p}&nbsp;({scores[p]})</a>\n            </p>\n'
                  for p in pkgs[c * k:(c + 1) * k]]
            h.append("        </div>\n")
        h.append("    </div>\n")
    h.append("</BODY>\n</HTML>\n")
    return "".join(h)


@functools.lru_cache(maxsize=None)
def listed_in(idx, v, cat):
    f = idx / f"{v}-{cat}.txt"
    return frozenset(f.read_text().split()) if f.exists() else frozenset()


def package_notice(cat, pkg, idx):
    """Release/devel links, or master's "not in the current release" line, from the cached PACKAGES indexes."""
    path = CATEGORIES[cat][1]
    link = lambda v: f'<a href="/packages/{v}/{path}/html/{pkg}.html">{v} version</a>'
    if idx:
        vs = sorted({f.name.split("-", 1)[0] for f in idx.glob("*-bioc.txt")}, key=lambda v: tuple(map(int, v.split("."))))
        listed = [v for v in vs if pkg in listed_in(idx, v, cat)]
        if len(vs) > 1 and vs[-2] not in listed:   # vs[-1] is devel, vs[-2] the current release
            return (f"Package {pkg} is not in the current release of Bioconductor. It was last seen in {listed[-1]}."
                    if listed else "")
    return f"<b>{pkg}</b> home page:\n            {link('release')},\n            {link('devel')}."


def write_pages(tree, idx=None, dest=None):
    """The stage 4 pages, from the .tab files of `tree`, into `dest` (default: tree). idx: packages-index/."""
    dest = dest or tree
    as_of = dt.date.fromisoformat((tree / "DATA_AS_OF").read_text().strip())
    as_of_text = f"{as_of:%a}. {as_of.day} {as_of:%b %Y}"   # "Thu. 15 Oct 2026"
    t0, n = time.time(), 0

    def put(path, text):
        nonlocal n
        path = dest / path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        n += 1

    for cat, (prefix, _) in CATEGORIES.items():
        page, kind, _k = KIND[cat]
        scores = {r[0]: int(r[1]) for r in read_tab(tree / cat / f"{prefix}_pkg_scores.tab")}
        put(page, listing_page(cat, scores, as_of_text))
        back = (f"/packages/stats/{page}", f"Download stats for Bioconductor {kind} packages")
        put(f"{cat}/index.html", stats_page(f"Download stats for Bioconductor {kind} repository (all packages combined)",
                                            back, "", year_blocks(tree / cat / f"{cat}_stats.tab"), cat, as_of_text))
        for d in sorted(p for p in (tree / cat).iterdir() if p.is_dir()):
            notice = package_notice(cat, d.name, idx)
            intro = f'<p style="text-align: center">\n        {notice}\n    </p>' if notice else ""
            put(f"{cat}/{d.name}/index.html", stats_page(f"Download stats for {'software' if cat == 'bioc' else kind} package {d.name}",
                                                          back, intro, year_blocks(d / f"{d.name}_stats.tab"), d.name, as_of_text))
    # ponytail: the listing pages are rewritten whole each run; incremental only if the tree outgrows seconds.
    put("static/barchart.js", (HERE / "static" / "barchart.js").read_text())
    print(f"  pages: {n:,d} files, data as of {as_of}  {time.time() - t0:.0f}s", flush=True)


def run(out, rebuild_all, salt, published):
    con = connect(salt)
    print("partitions:", flush=True)
    build_partitions(con, out, rebuild_all)
    print("aggregates:", flush=True)
    load_repo_downloads(con, out, package_universe(out))
    write_aggregates(con, out, published)
    write_stats(con, out, published)


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
        da = out / "stats" / "data-annotation"   # category totals take the directory name, as live
        assert (da / "data-annotation_stats.tab").exists() and (da / "data-annotation_2026_stats.tab").exists() and (da / "annotation_pkg_stats.tab").exists() and not (da / "annotation_stats.tab").exists()
        assert not (out / "stats" / "workflows" / "limma").exists(), "not in the workflows universe"

        # Published history: months before JOIN, tree 'stats', non-zero; the rest ignored.
        pub = out / "published_month.parquet"
        con.execute(f"""COPY (FROM (VALUES ('stats', 'bioc', 'limma', 2019, 12, 5, 7),
            ('stats', 'bioc', NULL, 2019, 12, 9, 20), ('stats', 'data-annotation', NULL, 2019, 12, 4, 6),
            ('stats', 'bioc', 'limma', 2020, 1, 99, 99), ('oldstats', 'bioc', 'limma', 2018, 1, 1, 1),
            ('stats', 'bioc', 'limma', 2019, 11, 0, 0))
            v(tree, category, package, year, month, distinct_ips, downloads)) TO '{pub}' (FORMAT parquet)""")
        write_aggregates(con, out, pub)
        got = {n: con.execute(f"SELECT * EXCLUDE (source) FROM '{out}/{n}.parquet' WHERE source = 'published'").fetchall()
               for n in HISTORY}
        none = (None,) * 8
        assert got == {"package_month": [(dt.date(2019, 12, 1), "bioc", "limma", 7, 5, *none)],
                       "category_month": [(dt.date(2019, 12, 1), "bioc", 20, 9, *none),
                                          (dt.date(2019, 12, 1), "data-annotation", 6, 4, *none)],
                       "overall_month": [(dt.date(2019, 12, 1), 26, None, *none)]}, got
        assert con.execute(f"SELECT count(*) FROM '{out}/package_month.parquet' WHERE source = 'logs' "
                           "AND month = DATE '2026-02-01'").fetchone()[0] == 3
        # The tree's years before JOIN: published months, and the year row as published.
        con.execute(f"""COPY (FROM (VALUES ('stats', 'bioc', 'limma', 2019, 6, 12), ('stats', 'bioc', NULL, 2019, 8, 20),
            ('stats', 'bioc', 'limma', 2020, 99, 99), ('stats', 'bioc', 'gone', 2010, 1, 2))
            v(tree, category, package, year, distinct_ips, downloads)) TO '{out}/published_year.parquet' (FORMAT parquet)""")
        try:
            write_stats(con, out, out / "missing.parquet")
            raise AssertionError("a missing published snapshot must fail, not truncate the tree")
        except SystemExit:
            pass
        assert (s / "limma" / "limma_stats.tab").exists(), "the refused run left the old tree in place"
        write_stats(con, out, pub)
        lines = (s / "limma" / "limma_stats.tab").read_text().split("\n")
        assert len(lines) == 40 and lines[27:] == [
            *(f"2019\t{m}\t0\t0" for m in MONTHS[:10]), "2019\tNov\t0\t0", "2019\tDec\t5\t7", "2019\tall\t6\t12"], lines
        assert (s / "limma" / "limma_2019_stats.tab").exists() and (s / "gone" / "gone_2010_stats.tab").exists()
        assert (s / "bioc_stats.tab").read_text().split("\n")[-1] == "2010\tall\t0\t0", "category years run back to the first"
        assert (s.parent / "data-annotation" / "data-annotation_stats.tab").read_text().split("\n")[-1] == "2019\tall\t0\t0", \
            "each category back to its own first year"
        assert (s / "bioc_2019_stats.tab").read_text().split("\n")[-2:] == ["2019\tDec\t9\t20", "2019\tall\t8\t20"]

        # Stage 4: write_stats wrote the pages before its swap. limma has 2026, 2025 and 2019 (published) blocks.
        t = out / "stats"
        assert not (out / "stats.partial").exists() and (t / "index.html").exists(), "pages swapped in with the tree"
        assert (t / "static" / "barchart.js").read_bytes() == (HERE / "static" / "barchart.js").read_bytes()
        page = (s / "limma" / "index.html").read_text()
        assert re.findall(r'<canvas id="(barchart_\d+)" class="barchart" width="720" height="400">', page) == \
            ["barchart_2026", "barchart_2025", "barchart_2019"], "one canvas per year with data"
        assert page.count("<td style=\"text-align: right\">") == 3 * 13 and "<td style=\"text-align: right\">Feb/2026</td>" in page
        assert re.search(r"<td [^>]*#aaaaff[^>]*>\s*2\s*</td>\s*<td [^>]*#ddddff[^>]*>\s*3\s*</td>", page), "Feb 2026: 2 IPs, 3 downloads"
        assert f"<i>Data as of {dt.date(2026, 2, 20):%a}. 20 Feb 2026</i>" in page, "the DATA_AS_OF line"
        assert re.findall(r'href="([^"/]+\.tab)"', page) == ["limma_2026_stats.tab", "limma_2025_stats.tab",
                                                              "limma_2019_stats.tab", "limma_stats.tab"]
        assert all((s / "limma" / f).exists() for f in re.findall(r'href="([^"/]+\.tab)"', page)), "relative links resolve"
        assert f'<script src="{CHARTJS}"></script>' in page and "@" in CHARTJS and \
            '<script type="module" src="/packages/stats/static/barchart.js"></script>' in page
        assert '"2019": [{"downloads": 0, "month": "Jan", "unique_ips": 0, "year": 2019}' in page
        assert (t / "bioc" / "index.html").exists() and (t / "data-annotation" / "org.Hs.eg.db" / "index.html").exists()
        assert not (t / "workflows" / "limma").exists() and (t / "workflows" / "index.html").exists()
        top = (t / "index.html").read_text()
        assert 'HREF="data-annotation.html"' in top and 'HREF="index.html"' not in top   # the nav lists the others
        pop = listing_page("data-annotation", {"b": 5, "A": 9, "c": 5, "d": 1}, "x")   # a populated listing
        assert 'HREF="index.html"' in pop and "Top 30" in pop and pop.index(">A&nbsp;(9)") < pop.index(">b&nbsp;(5)") < pop.index(">c&nbsp;(5)")
        assert pop.count('<div class="column">') == 3 * 4 and 'href="data-annotation/A/"' in pop
        assert "<h2>All workflow packages</h2>" not in (t / "workflows.html").read_text()
        assert ">Back to the" in (t / "data-annotation" / "index.html").read_text()
    print("self-check OK (downloads view = DOWNLOADS_SQL filter; aggregates; published history; stats tree format; pages)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=OUT, type=pathlib.Path, help=f"output directory (default {OUT})")
    ap.add_argument("--all", action="store_true", help="rebuild every monthly partition")
    ap.add_argument("--published", default=PUBLISHED, type=pathlib.Path,
                    help=f"published_month.parquet for the months before {JOIN} (default {PUBLISHED})")
    ap.add_argument("--self-check", action="store_true", help="run the built-in checks and exit")
    a = ap.parse_args()
    if a.self_check:
        return self_check()
    run(a.out, a.all, ip_salt(), a.published)


if __name__ == "__main__":
    sys.exit(main())
