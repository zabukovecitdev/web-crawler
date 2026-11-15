# Mutation testing

Stryker.NET mutates `SamoBot.Core` and runs the unit tests (`Category!=Integration`) against each mutant.
Config: `src/SamoBot.Tests/stryker-config.json`. CI: `.github/workflows/mutation.yml` (weekly in full,
on pull requests only for files changed against the base branch). The run fails below a score of 75 %.

```bash
dotnet tool restore
cd src/SamoBot.Tests
dotnet tool run dotnet-stryker                       # everything, about 6 minutes on 4 cores
dotnet tool run dotnet-stryker --since:origin/main   # changed files only
```

The report is written to `src/SamoBot.Tests/StrykerOutput/<timestamp>/reports/mutation-report.html`.

## Baseline (2026-11-15)

Score **79.17 %**: 796 killed, 6 timeout, 188 survived, 23 not covered (155 compile errors and
173 ignored mutants are not counted).

Gaps worth closing first:

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
