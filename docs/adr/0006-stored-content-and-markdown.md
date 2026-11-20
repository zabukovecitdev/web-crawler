# 0006. Stored content and Markdown

Status: Accepted

## Decision

**Which bodies are stored (`StoredMediaTypes`, `FetchResponse`).**
- `StoredMediaTypes` is the only place that says which media types the crawler stores and parses: HTML (`text/html`,
  `application/xhtml+xml`), Markdown (`text/markdown` from RFC 7763, and the older `text/x-markdown`) and `text/plain`.
  Other types are fetched and skipped.
- A missing or wrong media type falls back to a look at the first bytes (`LooksLikeHtml`).
- `HttpFetcher.Accept` asks for HTML first, then Markdown, then plain text, with `*/*` last. A server that negotiates on Accept
  (`Vary: Accept`) then sends HTML when it has some.

**Body cap (`HttpFetcher.MaxBodyBytes`, `FetchResponse.BodyTruncated`).** Both fetchers keep at most 5 MB of a body: the HTTP
fetcher stops reading there, and the browser fetcher cuts the rendered HTML. `BodyReader` probes one byte past the cap, so a body of
exactly 5 MB is not marked as cut. The fetch records `BodyTruncated`. The content and Markdown endpoints report the flag of the
stored copy they serve as `X-Content-Cut-At-Crawl`, which can be an older fetch than the latest one, so the dashboard never needs to
know the cap. Migration 028 marks older fetches of exactly 5 MB as cut, the best guess for them. Before that, only the HTTP fetcher
had a cap.

**Fetcher choice (`ChromeRenderingOptions.IsAvailable`).** The browser fetcher is in use only when it is enabled and has a
CDP endpoint. Otherwise the HTTP fetcher is used.

**One parser for every format (`HtmlAgilityPackParser`, `MarkdownPage`).**
- A Markdown body is first rendered to HTML, and plain text goes into a `<pre>` block, so every stored format goes through the
  same extractors. Plain text that starts like HTML is parsed as HTML.
- A Markdown title comes from the front matter `title`, else from the first level-one heading. The description comes from
  `description` or `summary`. Only top-level scalar front matter keys are read.
- When a page has several `<base href>` elements, only the first counts, as in browsers.

**HTML to Markdown (`ReverseMarkdownConverter`, `HtmlCleaner`, `MarkdownTidy`).**
- There are three steps: `HtmlCleaner` picks the main content, removes noise and makes URLs absolute. ReverseMarkdown converts.
  `MarkdownTidy` normalises line endings, spaces, blank lines and empty links.
- `HtmlCleaner` removes noise and cleans images in one walk. It cleans anchors after that walk, because an anchor is empty
  only once its images are known.
- `GithubFlavored = true` is used, not `Flavor = MarkdownFlavor.GitHub`. In ReverseMarkdown 6.3.0, the latter returns the HTML
  unconverted. The former gives the same pipe tables and fenced code.
- Each call creates a new `Converter`. The library does not document its thread safety, and the class is a singleton.
- `IMarkdownConverter` is pure and thread-safe. It resolves relative references against the page URL.

**Markdown endpoint (`PagesController`).**
- The endpoint makes Markdown on demand. `X-Content-Truncated` says that the HTML was cut at the size cap before conversion.
- A Markdown or plain-text body is already readable, so the endpoint returns it unchanged. The HTML converter would escape its
  line starts and join its lines. The parser makes the same choice.

**Stored copy (`StoredCopy`).** The stored copy carries the address the body came from: the final URL after redirects, else the
page URL. It also carries the policy parsed from the body and the Content-Type.
