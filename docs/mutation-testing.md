# Mutation testing

Stryker.NET mutates `SamoBot.Core` and runs the unit tests (`Category!=Integration`) against each mutant.
Config: `src/SamoBot.Tests/stryker-config.json`. CI: `.github/workflows/mutation.yml` runs it weekly and on
demand and uploads the HTML report. It is report only: no score fails the run, and it does not run on pull requests.
Run it by hand after changing core logic (politeness, robots, URL normalization, sitemaps, directives).

```bash
dotnet tool restore
cd src/SamoBot.Tests
dotnet tool run dotnet-stryker                       # about 6 minutes on 4 cores
dotnet tool run dotnet-stryker --since:origin/main   # only files changed against main
```

The report is written to `src/SamoBot.Tests/StrykerOutput/<timestamp>/reports/mutation-report.html`.

## Baseline (2026-11-15)

Score **79.17 %**: 796 killed, 6 timeout, 188 survived, 23 not covered (155 compile errors and
173 ignored mutants are not counted).

Gaps found then:

| File | Undetected | What the survivors show |
|---|---|---|
| `Sitemaps/CappedReadStream.cs` | 9 / 12 | No test reads past the size limit: the decompression guard can be removed and nothing fails. |
| `Sitemaps/SitemapParser.cs` | 17 / 62 | Empty and oversized input, the gzip magic check and the `lastmod` child match are not pinned down. |
| `Crawling/RobotsGate.cs` | 10 / 21 | Caching is untested: removing `Remember` or `cache.Set` (fetch robots.txt on every URL) passes. |
| `Directives/Rules/DirectiveDate.cs` | 28 / 43 | Only a few date formats and no named time zones (EST, PST, CET ...) are tested; a wrong sign passes. |
| `Common/UrlNormalizer.cs` | 16 / 78 | Single tracking parameters (`utm_*`, `clickid`) are not tested one by one. The default-port branch may be redundant, because `Uri` already drops default ports. |
| `Crawling/InFlightHold.cs` | 3 / 20 | Boundaries (`>=` vs `>`) at equal timeouts and at the cap. |

Most of the remaining survivors are in `Domain` records (`SearchHit`, `PageToIndex`, `PageDetail`,
`ParsedPage`), which are data carriers with little logic.

## After closing the high-value gaps (2026-11-15)

Score **84.67 %**: 828 killed, 6 timeout, 130 survived, 21 not covered. Tests were added for the sitemap size
cap, robots.txt caching, sitemap size boundaries and extensions, and every zone name and format of directive dates.
`InFlightHold` and `UrlNormalizer` lost code that could not change any result.

The survivors left in those files change nothing a test can observe, so they are accepted:

- `SitemapParser`: the reader settings and checks are layered (the defaults of .NET are already safe, and the
  size cap and the byte limit back each other up), so removing one layer alone changes no result.
- `DirectiveDate`: `UTC` and `GMT` are also understood by `DateTimeOffset.TryParse`; `AssumeUniversal` only
  matters on a machine whose local zone is not UTC.
- `RobotsGate`: `&&` to `||` in the cache check gives the same result for every value.
- `CappedReadStream`: the exception message and `base.Dispose`.
