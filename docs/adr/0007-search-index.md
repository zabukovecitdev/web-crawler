# 0007. Search index

Status: Accepted

## Decision

- **One document per URL.** The Meilisearch document id is the discovered URL id, which is also the page id on the dashboard.
  A recrawl parses the same URL again, so only the newest parsed document of each URL is sent and decides
  (`IndexBatchHandler.LatestPerUrl`, `MeilisearchIndex.LatestPerUrl`).
- **Removal follows the policy.** A page whose policy allows indexing is added or updated. Any other page (noindex, a copy of a
  canonical page, past its `unavailable_after` date) is removed. A site that adds noindex later disappears from search on the
  next crawl.
- **unavailable_after.** Search filters on `indexUntil > now`, so a page with no end stores `SearchHit.NoEnd`: the last second
  of the year 9999. Pages past their date never match. They stay in the index until the next crawl removes them.
- **Settings once per process (`MeilisearchSettings`).** The settings are applied before the first read or write. Applying the
  same settings again is a no-op for Meilisearch, so every service that touches the index may call `Ensure`. A failure is
  retried on the next call.
- **Filters (`MeilisearchPageSearch`).** The host filter value is a quoted string, so a backslash or a quote in a host cannot
  end it early. The host list ignores the host filter, so the dashboard can still offer the other hosts.
- **Snippets (`SnippetFormatter`).** The engine wraps matches in two private-use characters. The formatter encodes everything
  else, then swaps those characters for `<mark>` tags, so a snippet is encoded text plus mark tags only. The site's
  `max-snippet` limits the visible characters, and the markers do not count. 0 (nosnippet) gives an empty snippet.
  `SearchHitDto.Snippet` keeps that contract over the API.
- The property names of `SearchHit` are the field names of the Meilisearch index.
