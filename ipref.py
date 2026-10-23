#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb>=1.4"]
# ///
"""ASN and country by address for both eras, plus the crawlers' published ranges (#4).

    ./ipref.py [--release YYYY-MM]   # DB-IP Lite release (default: this month's) + crawler lists
    ./ipref.py --self-check

The CloudFront era logs only c_ip; the Cloudflare era adds cf_asn and cf_country. These
tables give both eras ASN and country from the address, and the Cloudflare fields become a
cross-check (sql/ipref_agreement.sql). Queried through sql/ipref.sql.

1. DB-IP Lite: IP to ASN Lite and IP to Country Lite (https://db-ip.com, CC BY 4.0), monthly.
   Each release lands whole and gzipped as <root>/raw/dbip/dbip-{asn,country}-lite-<release>.csv.gz
   and is kept: DB-IP has no deep archive, so CloudFront-era rows match against the earliest
   release (dbip_release_for). A landed release is not fetched again. With no --release:
   this month's, or last month's while this month's is not out (404). DB-IP serves the
   previous month's files too, so --release <last month> works once.

   <root>/dbip/dbip-<release>.parquet is the two files merged into non-overlapping ranges
   covering each address space (NULLs where DB-IP has nothing), with a dbip_release column,
   so one ASOF join is an exact lookup. Addresses are UBIGINT hi/lo halves: DuckDB writes
   UHUGEINT to Parquet as DOUBLE. Built only if missing; delete it to rebuild.

2. Crawler lists (CRAWLERS), fetched every run: <root>/raw/crawlers/<date>/<operator>-<list>.json
   and <root>/crawlers/crawlers-<date>.parquet, one row per published prefix. The
   CloudFront-era stand-in for cf.verifiedBotCategory.
"""

import argparse, datetime as dt, gzip, ipaddress, json, pathlib, sys, tempfile, urllib.error, urllib.request
import duckdb

HERE = pathlib.Path(__file__).resolve().parent
ROOT = pathlib.Path("/data/davsean/bioc-traffic-ipref")
SCRATCH = pathlib.Path("/data/davsean/tmp")
DBIP = "https://download.db-ip.com/free/dbip-{kind}-lite-{release}.csv.gz"
# Cloudflare-fronted sites answer 403 to Python-urllib's default User-Agent.
UA = "Mozilla/5.0 (compatible; bioc-traffic ipref; +https://github.com/seandavi/bioc-traffic)"
# (operator, list, url). All share Google's shape: {"creationTime", "prefixes": [{"ipv4Prefix"
# or "ipv6Prefix"}]}. Anthropic's is the one its crawler help article links.
CRAWLERS = [
    ("google", "googlebot", "https://developers.google.com/static/search/apis/ipranges/googlebot.json"),
    ("google", "special-crawlers", "https://developers.google.com/static/search/apis/ipranges/special-crawlers.json"),
    ("google", "user-triggered-fetchers", "https://developers.google.com/static/search/apis/ipranges/user-triggered-fetchers.json"),
    ("google", "user-triggered-fetchers-google", "https://developers.google.com/static/search/apis/ipranges/user-triggered-fetchers-google.json"),
    ("bing", "bingbot", "https://www.bing.com/toolbox/bingbot.json"),
    ("openai", "gptbot", "https://openai.com/gptbot.json"),
    ("openai", "searchbot", "https://openai.com/searchbot.json"),
    ("openai", "chatgpt-user", "https://openai.com/chatgpt-user.json"),
    ("anthropic", "bots", "https://claude.com/crawling/bots.json"),
]
TOP = "CASE v WHEN 4 THEN 4294967295::UHUGEINT ELSE 340282366920938463463374607431768211455::UHUGEINT END"


def connect():
    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{SCRATCH}/duckdb-ipref'")
    con.execute((HERE / "sql" / "ipref.sql").read_text())
    return con


def fetch(url):
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": UA}), timeout=120) as r:
        return r.read()


def land(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".partial")
    tmp.write_bytes(body)
    tmp.rename(path)


def split(col, name):
    """A UHUGEINT as the hi/lo UBIGINT halves Parquet can hold (ip_join_ in sql/ipref.sql)."""
    return (f"({col} >> 64)::UBIGINT AS {name}_hi, "
            f"({col} & 18446744073709551615::UHUGEINT)::UBIGINT AS {name}_lo")


# --- 1. DB-IP Lite ---------------------------------------------------------------------

def dbip_raw(root, kind, release):
    return root / "raw" / "dbip" / f"dbip-{kind}-lite-{release}.csv.gz"


def land_dbip(root, release):
    """Fetch the release's two files unless landed. False if DB-IP does not have it (404)."""
    for kind in ("asn", "country"):
        path = dbip_raw(root, kind, release)
        if path.exists():
            continue
        try:
            body = fetch(DBIP.format(kind=kind, release=release))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return False
            raise
        if not body.startswith(b"\x1f\x8b"):
            sys.exit(f"{path.name}: not gzip, refusing to land it")
        land(path, body)
        print(f"landed {path} ({len(body):,} bytes)")
    return True


def build_dbip(con, root, release):
    """<root>/dbip/dbip-<release>.parquet from the landed CSVs."""
    out = root / "dbip" / f"dbip-{release}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    cols = {"asn": "{'f': 'VARCHAR', 'l': 'VARCHAR', 'asn': 'BIGINT', 'as_org': 'VARCHAR'}",
            "country": "{'f': 'VARCHAR', 'l': 'VARCHAR', 'country': 'VARCHAR'}"}
    for kind in ("asn", "country"):
        con.execute(f"""CREATE OR REPLACE TEMP TABLE {kind} AS
            SELECT ip_version(f) AS v, ip_num(f) AS lo, ip_num(l) AS hi, * EXCLUDE (f, l)
            FROM read_csv('{dbip_raw(root, kind, release)}', header = false, quote = '"',
                          columns = {cols[kind]})""")
        # The merge below assumes each file's ranges are well-formed and do not overlap.
        bad = con.execute(f"""SELECT count(*) FROM (
            SELECT lo, hi, v, lag(hi) OVER (PARTITION BY v ORDER BY lo) AS prev FROM {kind})
            WHERE v IS NULL OR lo IS NULL OR hi IS NULL OR lo > hi OR lo <= prev""").fetchone()[0]
        if bad:
            sys.exit(f"dbip {kind} {release}: {bad} malformed or overlapping ranges")
    con.execute(f"""COPY (
        WITH b AS (
            SELECT v, lo AS p FROM asn UNION SELECT v, hi + 1 FROM asn WHERE hi < {TOP}
            UNION SELECT v, lo FROM country UNION SELECT v, hi + 1 FROM country WHERE hi < {TOP}
            UNION SELECT * FROM (VALUES (4, 0::UHUGEINT), (6, 0::UHUGEINT))),
        seg AS (
            SELECT v, p AS ip_first, coalesce(lead(p) OVER (PARTITION BY v ORDER BY p) - 1, {TOP}) AS ip_last
            FROM b)
        SELECT '{release}' AS dbip_release, s.v AS ip_version,
               {split('s.ip_first', 'first')}, {split('s.ip_last', 'last')},
               CASE WHEN s.ip_first <= asn.hi THEN asn.asn END AS asn,
               CASE WHEN s.ip_first <= asn.hi THEN asn.as_org END AS as_org,
               CASE WHEN s.ip_first <= country.hi THEN country.country END AS country
        FROM seg s
        ASOF LEFT JOIN asn ON s.v = asn.v AND s.ip_first >= asn.lo
        ASOF LEFT JOIN country ON s.v = country.v AND s.ip_first >= country.lo
        ORDER BY ip_version, s.ip_first
    ) TO '{out}.partial' (FORMAT parquet, COMPRESSION zstd)""")
    pathlib.Path(f"{out}.partial").rename(out)
    n = con.execute(f"SELECT count(*) FROM '{out}'").fetchone()[0]
    print(f"wrote {out} ({n:,} ranges)")


# --- 2. Crawler lists ------------------------------------------------------------------

def crawler_rows(operator, name, doc):
    rows = []
    for p in doc["prefixes"]:
        prefix = p.get("ipv4Prefix") or p.get("ipv6Prefix")
        net = ipaddress.ip_network(prefix, strict=False)
        rows.append({"operator": operator, "list": name, "creation_time": doc.get("creationTime"),
                     "prefix": prefix, "first": str(net.network_address), "last": str(net.broadcast_address)})
    if not rows:
        sys.exit(f"{operator}-{name}: no prefixes, has the format changed?")
    return rows


def land_crawlers(con, root, date):
    rows = []
    for operator, name, url in CRAWLERS:
        path = root / "raw" / "crawlers" / date / f"{operator}-{name}.json"
        if not path.exists():
            land(path, fetch(url))
        rows += crawler_rows(operator, name, json.loads(path.read_bytes()))
    out = root / "crawlers" / f"crawlers-{date}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""COPY (
        SELECT DATE '{date}' AS fetched, operator, list, creation_time, prefix, ip_version(first) AS ip_version,
               {split('ip_num(first)', 'first')}, {split('ip_num(last)', 'last')}
        FROM (SELECT unnest($rows, recursive := true))
    ) TO '{out}.partial' (FORMAT parquet, COMPRESSION zstd)""", {"rows": rows})
    pathlib.Path(f"{out}.partial").rename(out)
    print(f"wrote {out} ({len(rows):,} prefixes)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--release", type=lambda s: f"{dt.datetime.strptime(s, '%Y-%m'):%Y-%m}",
                    help="DB-IP release, YYYY-MM (default: this month's, else last month's)")
    ap.add_argument("--root", default=ROOT, type=pathlib.Path, help=f"default {ROOT}")
    ap.add_argument("--self-check", action="store_true")
    a = ap.parse_args()
    if a.self_check:
        return self_check()
    today = dt.datetime.now(dt.timezone.utc).date()
    first = today.replace(day=1)
    for release in [a.release] if a.release else [f"{first:%Y-%m}", f"{first - dt.timedelta(days=1):%Y-%m}"]:
        if land_dbip(a.root, release):
            break
        print(f"dbip {release}: not published (404)")
    else:
        sys.exit("no DB-IP release to load")
    con = connect()
    if not (a.root / "dbip" / f"dbip-{release}.parquet").exists():
        build_dbip(con, a.root, release)
    land_crawlers(con, a.root, today.isoformat())


def self_check():
    import random
    con = connect()
    # ip_num = Python's ipaddress, both versions, compressed and exploded text.
    rnd = random.Random(4)
    addrs = [ipaddress.ip_address(a) for a in ("0.0.0.0", "255.255.255.255", "1.2.3.4", "::", "::1",
                                               "1::", "2001:db8::1", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff")]
    addrs += [ipaddress.IPv4Address(rnd.getrandbits(32)) for _ in range(200)]
    addrs += [ipaddress.IPv6Address(rnd.getrandbits(128) >> rnd.choice((0, 16, 64, 100))) for _ in range(200)]
    texts = [(str(a), a.version, int(a)) for a in addrs]
    texts += [(a.exploded, a.version, int(a)) for a in addrs if a.version == 6]
    texts += [("::ffff:1.2.3.4", 4, 16909060)]
    got = con.execute("SELECT t.ip, ip_version(t.ip), ip_num(t.ip)::VARCHAR FROM (SELECT unnest($t) AS ip) t",
                      {"t": [t for t, _, _ in texts]}).fetchall()
    assert [(t, v, str(n)) for t, v, n in texts] == got, [x for x in zip(texts, got) if str(x[0][2]) != x[1][2]][:3]
    bad = con.execute("SELECT list(ip_num(ip)) FROM (SELECT unnest(['1.2.3', '256.1.1.1', '1.2.3.4.5', "
                      "'1::2::3', 'zz::', '1:2:3:4:5:6:7:8:9', '12345::', 'x']) AS ip)").fetchone()[0]
    assert bad == [None] * 8, bad

    # The merged release = a brute-force lookup in the two CSVs, gaps and overlaps of the
    # two files' boundaries included; then dbip_release_for over two releases.
    asn = [("1.0.0.0", "1.0.0.255", 13335, "Cloudflare, Inc."), ("1.0.4.0", "1.0.7.255", 38803, "Gtelecom"),
           ("2001::", "2001:0:ffff:ffff:ffff:ffff:ffff:ffff", 6939, "Hurricane Electric LLC")]
    country = [("0.0.0.0", "0.255.255.255", "ZZ"), ("1.0.0.0", "1.0.1.255", "AU"), ("1.0.2.0", "1.0.5.255", "CN"),
               ("::", "1fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "ZZ"), ("2001::", "2001:1::", "US"),
               ("fec0::", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "CH")]

    def csv(rows):
        return gzip.compress("".join(",".join(f'"{x}"' if isinstance(x, str) and "," in x else str(x)
                                              for x in r) + "\n" for r in rows).encode())

    def brute(ip, rows):
        a = ipaddress.ip_address(ip)
        return next((r[2:] for r in rows if a.version == ipaddress.ip_address(r[0]).version
                     and ipaddress.ip_address(r[0]) <= a <= ipaddress.ip_address(r[1])), None)

    with tempfile.TemporaryDirectory(dir=SCRATCH) as tmp:
        root = pathlib.Path(tmp)
        for release in ("2026-09", "2026-10"):
            land(dbip_raw(root, "asn", release), csv(asn))
            land(dbip_raw(root, "country", release), csv(country))
            build_dbip(con, root, release)
        con.execute(f"SET VARIABLE ipref_root = '{root}'")
        probes = ["0.0.0.0", "0.255.255.255", "1.0.0.0", "1.0.0.255", "1.0.1.0", "1.0.2.0", "1.0.3.9",
                  "1.0.5.255", "1.0.6.0", "1.0.8.0", "255.255.255.255", "::", "::1", "2000::", "2001::",
                  "2001:1::", "2001:1::1", "2001:0:ffff::", "fe80::1", "fec0::", "ffff::1"]
        got = con.execute("""SELECT p.ip, r.asn, r.as_org, r.country
            FROM (SELECT unnest($p) AS ip) p
            ASOF LEFT JOIN dbip('2026-10') r ON ip_version(p.ip) = r.ip_version AND ip_num(p.ip) >= r.ip_first
            ORDER BY ip_version(p.ip), ip_num(p.ip)""", {"p": probes}).fetchall()
        want = sorted(((ip, *(brute(ip, asn) or (None, None)), (brute(ip, country) or (None,))[0]) for ip in probes),
                      key=lambda r: (ipaddress.ip_address(r[0]).version, ipaddress.ip_address(r[0])))
        assert got == want, [x for x in zip(got, want) if x[0] != x[1]]
        cover = con.execute("""SELECT ip_version, min(ip_first)::VARCHAR, max(ip_last)::VARCHAR,
                                      count(*) FILTER (WHERE ip_first <> prev + 1)
                               FROM (SELECT *, lag(ip_last) OVER (PARTITION BY ip_version ORDER BY ip_first) AS prev
                                     FROM dbip('2026-10'))
                               GROUP BY 1 ORDER BY 1""").fetchall()
        assert cover == [(4, "0", str(2 ** 32 - 1), 0), (6, "0", str(2 ** 128 - 1), 0)], cover
        assert con.execute("SELECT DISTINCT dbip_release FROM dbip('*') ORDER BY 1").fetchall() == [("2026-09",), ("2026-10",)]
        got = con.execute("SELECT dbip_release_for(DATE '2020-01-01'), dbip_release_for(DATE '2026-09-30'), "
                          "dbip_release_for(DATE '2026-10-01'), dbip_release_for(DATE '2027-03-01')").fetchone()
        assert got == ("2026-09", "2026-09", "2026-10", "2026-10"), got

        rows = crawler_rows("openai", "gptbot", {"creationTime": "t", "prefixes": [
            {"ipv4Prefix": "20.125.66.80/28"}, {"ipv6Prefix": "2001:4860:4801:10::/64"}]})
        assert [(r["first"], r["last"]) for r in rows] == [
            ("20.125.66.80", "20.125.66.95"), ("2001:4860:4801:10::", "2001:4860:4801:10:ffff:ffff:ffff:ffff")], rows
    print("self-check OK")


if __name__ == "__main__":
    sys.exit(main())
