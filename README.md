# bioc-traffic

**Who actually uses Bioconductor?** This repo turns every request to
[bioconductor.org](https://bioconductor.org) since 2020 into answers: which packages get
installed, by whom, and how much of the traffic is people at all.

About 7.5 billion requests from the CloudFront years, plus around 6 million a day since the
site moved to a Cloudflare Worker in September 2026, queried as one dataset.

## Not all traffic is people

A raw request count says little. So every request gets a **traffic class** from a versioned,
rule-based classifier:

> human browser · R / package client · mirror · CI · search crawler · AI crawler ·
> monitoring · likely automated

Some of what it turns up:

- **Most package downloads come from R, not browsers.** In July 2026, 81% of package
  downloads were R and related package clients, and about 10% came from browsers.
- **People are a minority of web traffic.** In one day of late-September traffic, browsers
  were about 20% of requests, R clients 31%, search and AI crawlers 10%, and other automated
  clients 34%.
- **Crawlers hide behind browser user agents.** In the first 19 minutes after the switch to
  Cloudflare, one crawler made 75% of all requests. It used ordinary Chrome user agents, sent
  from a cloud provider. Its crawl pattern was already 24% of all requests in July.
- **New ones show up.** A crawler fetching from Google Cloud now makes about a million
  requests a day, mostly build-report pages.

The classifier is at v0 and the rules are in plain SQL (`sql/client_class.sql`). Every
number it produces carries its rule version, so history recomputes when the rules improve.

## What it produces

- **Package download statistics, 2020 to today, across both eras.** Counts that match the
  long-published `/packages/stats/` figures, plus separate human, package-client and
  automated columns.
- **A traffic dashboard's worth of rollups.** Per minute, hour and day, by class, status,
  country, page, referrer and package. A public dashboard built from them is next
  ([#6](../../issues/6)).

## Privacy

The logs contain client IP addresses, and they never leave this pipeline. Distinct clients are
counted with a salted hash, and only aggregates are published. No log data is in this
repository.

## Related

- [bioc-edge](https://github.com/seandavi/bioc-edge): the Cloudflare Worker that serves
  bioconductor.org and writes the access records.
- [bioc-infrastructure](https://github.com/seandavi/bioc-infrastructure): the decisions
  behind it (ADRs), and how this fits with the package registry
  ([#67](https://github.com/seandavi/bioc-infrastructure/issues/67)).

Apache 2.0.
