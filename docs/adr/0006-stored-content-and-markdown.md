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
- `HttpFetcher` reads at most 5 MB of a body (`MaxBodyBytes`) and stops reading at that limit.

**Object names (`ObjectNameGenerator`, `MinioContentStore`).**
- The object name comes from the URL (host, sanitised path and a SHA-256 of the URL), not from the fetch. A recrawl overwrites
  the object, so every `UrlFetches` row of a URL points at the newest body.
- `MinioContentStore.Save` gives every name the `.html` extension, whatever the type. The stored Content-Type tells the formats apart.

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
- ReverseMarkdown (MIT) is the one new dependency, because the conversion has the most edge cases (escaping, nested lists,
  tables, code). Cleaning needs no library: HtmlAgilityPack is already in `Adapters`, so the rules are ours and tested.
- Readability (SmartReader) is not used. The simple root rule never drops a whole page, and a Readability step can be added
  inside the adapter later without a change to any caller. Hosted converters (Jina Reader, Firecrawl) were rejected: they
  fetch the URL again themselves, so the result is not the crawled copy, the URL goes to a third party, and the fetch bypasses
  the address policy (ADR 0002).
- The root is the first `<main>`, else the `<article>` when there is exactly one, else `<body>`. `<header>` is kept, because it
  often holds the `<h1>` of the article. Navigation, side panels, footers, forms, scripts, styles, embedded objects, hidden
  elements (`hidden`, `aria-hidden="true"`, `display:none`) and landmark roles such as `navigation` and `banner` are removed.
- An image without a usable `src` takes the first of `data-src`, `data-lazy-src` and `data-original`. An image with no http or
  https URL is removed. A link that is a fragment (`#...`) or uses a scheme other than http, https or mailto keeps only its text.
- `HtmlCleaner` removes noise and cleans images in one walk. It cleans anchors after that walk, because an anchor is empty
  only once its images are known.
- `GithubFlavored = true` is used, not `Flavor = MarkdownFlavor.GitHub`. In ReverseMarkdown 6.3.0, the latter returns the HTML
  unconverted. The former gives the same pipe tables and fenced code.
- Each call creates a new `Converter`. The library does not document its thread safety, and the class is a singleton.
- `IMarkdownConverter` is pure and thread-safe. It resolves relative references against the page URL.

**Markdown endpoint (`PagesController`).**
- The endpoint makes Markdown on demand. Nothing is stored, so there is no migration and no backfill, every page benefits from
  a better converter at once, and the `noarchive` check of `/content` applies unchanged. A later embedding or LLM stage calls the
  same `IMarkdownConverter` and can store its result then (ADR 0016).
- The body is decoded as UTF-8, as the parser does. Other charsets are not supported.
- `X-Content-Truncated` says that the HTML was cut at the size cap (`StoredContent.MaxBytes`, 2 MB) before conversion.
- A Markdown or plain-text body is already readable, so the endpoint returns it unchanged. The HTML converter would escape its
  line starts and join its lines. The parser makes the same choice.

**Stored copy (`StoredCopy`).** The stored copy carries the address the body came from: the final URL after redirects, else the
page URL. It also carries the policy parsed from the body and the Content-Type.
