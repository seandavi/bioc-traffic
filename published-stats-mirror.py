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
   map. Files land byte for byte under <root>/raw/<date>/<URL path> (a directory URL
   is its index.html, which the server answers identically), with manifest.jsonl beside
   them: url, path, status, bytes, sha256, content_type, last_modified, fetched_at. A
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

import argparse, concurrent.futures as cf, datetime as dt, hashlib, json, pathlib, re, sys, time
import urllib.error, urllib.parse, urllib.request

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
           "content_type": headers.get("Content-Type"), "last_modified": headers.get("Last-Modified"),
           "location": headers.get("Location"),
           "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")}
    if status == 200:
        f = snap / row["path"]
        f.parent.mkdir(parents=True, exist_ok=True)
        tmp = f.with_name(f.name + ".partial")
        tmp.write_bytes(body)
        tmp.replace(f)
    return row


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
                        new = [q for q in links(p, (snap / r["path"]).read_bytes()) if q not in seen]
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
                    new = [q for q in links(p, (snap / r["path"]).read_bytes()) if q not in seen]
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
    print(f"snapshot {snap}: {len(ok):,d} files, {sum(r['bytes'] for r in ok):,d} bytes; "
          f"statuses {by_status}; fetched {fetched:,d} this run in {time.time() - t0:.0f}s", flush=True)
    for t in TREES:
        tr = [r for r in ok if ("/" + r["path"]).startswith(t)]
        print(f"  {t}: {len(tr):,d} files, {sum(r['bytes'] for r in tr):,d} bytes", flush=True)
    if failed:
        print(f"{len(failed)} paths failed after {TRIES} tries (rerun to retry):", *failed[:20],
              sep="\n  ", file=sys.stderr)
        return 1


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
    ap.add_argument("--self-check", action="store_true")
    a = ap.parse_args()
    if a.self_check:
        return self_check()
    if a.step == "crawl":
        return crawl(a.root, a.date or dt.datetime.now(dt.timezone.utc).date().isoformat())
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
