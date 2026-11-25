# 0005. Crawl directives

Status: Accepted

## Context

Sites say what a crawler may do in robots.txt, robots meta tags, the X-Robots-Tag header, rel values, canonical links and
meta refresh. The README section "Crawl directives" lists the extension points. These are the decisions behind them.

## Decision

**Composition (`DirectivesServiceCollectionExtensions.AddCrawlDirectives`).** This is the only place to change what the
crawler obeys: add, remove or reorder a source or a rule there. `AddDirective` registers each object under every directive
interface it implements. `ParsePageHandler` decides nothing itself.

**Extension points (`Abstractions.cs`).**
- `IRobotsDirectiveSource`: a place a site can send directives (meta tag, header). It returns only the directives that apply to
  this crawler. A new source changes no rule.
- `IPagePolicyRule`: one decision. It receives the policy so far and returns it, possibly stricter. A new rule changes no other
  rule and not the evaluator.
- `IRedirectSource`: URLs a page offers in place of itself (meta refresh, canonical). They are queued like an HTTP redirect: at
  the depth of the page, whether or not its links may be followed.
- `ICanonicalSource`: the first source that finds a canonical URL wins, in registration order.
- `IResponseHeaderConsumer`: a source that reads response headers names them, and the fetchers keep exactly the union of
  those headers (`ResponseHeaderSelection`). Header names are case-insensitive, and a header sent twice has two values.

**Policy (`PagePolicy`, `PagePolicyEvaluator`).**
- The defaults allow everything. A rule can only make the policy stricter.
- The evaluator knows no directive by name. What a directive means lives in its rule.

| Field | Meaning | Set by |
|---|---|---|
| `FollowLinks` false | no link of the page is queued | `nofollow`, `none` |
| `Index` false | kept out of the search index, and removed if it was indexed | `noindex`, `none`, a canonical that names another page |
| `IndexUntil` | the page leaves the index at that time | `unavailable_after` (a date that cannot be read is ignored) |
| `SnippetMaxLength` | null: no limit; 0: no snippet; -1 or invalid: no limit | `max-snippet: N`, `nosnippet` |
| `Archive` false | the stored copy is kept for parsing but never shown | `noarchive` |

**Parsing (`RobotsDirectiveParser`, `DirectiveDate`).**
- Directives match as whole names, never by substring.
- A directive with a value is `name: value`. A listed set of directives carries values, so `name: value` is never read as a
  crawler name.
- A value that contains a comma (an RFC 850 date after `unavailable_after`) is joined back, because no directive name starts
  with a digit.
- An X-Robots-Tag value can start with a crawler name (`googlebot: nofollow`). The name applies to the directives after it,
  up to the next name. Only directives without a name, or for our user agent, are returned.
- `unavailable_after` dates may be ISO 8601, RFC 822/1123 or RFC 850, with a numeric offset or a common zone name (GMT, UTC,
  PST, EST, ...). A date without a zone is read as UTC.

**Sources.** `<meta name="robots">` applies to every crawler and `<meta name="samobot">` to this one, matched without regard to
case. The X-Robots-Tag value may target one crawler, and values for other crawlers are ignored. `ParsedPage.MetaTags` keeps every
meta tag as written. Interpreting them is the job of the sources. The crawler's name is `SamoBot` (`RobotsGate.UserAgent`,
`CrawlerIdentity.Name`): robots.txt groups, `<meta name="samobot">` and X-Robots-Tag scopes match it. Requests send the
`User-Agent` `SamoBot/1.0 (+https://github.com/zabukovecitdev/web-crawler)` (`HttpFetcher.UserAgent`).

**Links (`LinkSelector`, `RelLinkRule`, `NoFollowRule`).**
- Links are queued one level deeper when the policy allows following and every `ILinkFollowRule` allows the link.
- Redirect targets are queued at the depth of the page, whatever the policy says about links. `nofollow` does not stop meta
  refresh or canonical targets. The job scope applies to both.
- A URL that is both a redirect target and a link keeps the redirect entry, at the depth of the page.
- The blocked rel values (`nofollow`, `sponsored`, `ugc`) are chosen at registration, so following sponsored or ugc links is a
  registration change.

**Canonical and base URL (`PageContextFactory`, `LinkHeaderCanonicalSource`, `NonCanonicalPageRule`, `CanonicalTargetSource`).**
- A `<base href>` changes what relative links resolve against, and is itself relative to the page URL.
- The HTTP `Link: <...>; rel="canonical"` header (RFC 8288) is how PDFs and other non-HTML files name a canonical. Its relative
  target resolves against the page URL, not the `<base>` of the HTML.
- A page whose canonical is another page is a copy: it is not indexed, so search shows only the main version. The canonical
  target is queued so it gets crawled.
- A meta refresh with a URL is treated as a redirect. A refresh without a URL only reloads the page and is ignored.

**robots.txt is always obeyed (`RobotsGate`, migration 027).** Every URL is checked, and no job or setting can turn this off.
The rules come from the cache, then the store, then the source. A host with a blocked address disallows everything, and the
dashboard reads the same rules. Migration 027 removed the old switch: jobs created with it off are checked from then on.

## Consequences

Support for a new directive means one new rule or source, registered in `AddCrawlDirectives`, plus the place that enforces the
new policy field (the indexer or the API).
