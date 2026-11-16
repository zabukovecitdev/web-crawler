# Page Markdown plan

This plan adds a cleaning step that turns the stored HTML of a crawled page into clean Markdown.
The dashboard shows the result in a new **Markdown** tab on the page detail screen, next to the **HTML source** tab that already exists.
Later, an embedding or LLM stage can call the same converter.

Each task is small. It names the exact files and has a check. Agents can do the tasks (section 6).

## 0. How to use this plan

1. Read sections 1 to 5 before you start a task. Sections 1 to 3 give the reasons for the decisions. Do not change a decision. Report a problem with one instead.
2. Do the tasks in the order of their IDs. Track B (frontend) can run in parallel with track A (backend) after T0 (see 6.1).
3. After each task, run the check commands in section 5. Do not start the next task when a check fails.
4. Change only the files that the task names, plus tests.
5. If the code does not agree with this plan (a name, a signature, a library API), stop and report the difference. Do not guess.
6. Make one commit for each task. Start the message with the task ID, for example `M2 Add the HTML cleaner`.

## 1. Problem and goal

Today the stored copy of a page is its raw HTML. It is in RustFS/S3 and `GET /api/v1/pages/{id}/content` serves it as text.
The raw HTML has scripts, styles, navigation, footers, tracking pixels, inline SVG, forms and empty lines. A person cannot read the content, and it is poor input for an embedding model or an LLM.

`ParsedPage.BodyText` (stored in Postgres) is the flat text. It has no structure: it has no headings, lists, images or links, and the paragraph breaks are approximate.

**Goal:** a deterministic function `HTML + page URL → Markdown` that

- keeps headings (`#` to `######`), paragraphs, lists, block quotes, code, tables, links, and images with their alt text,
- removes scripts, styles, navigation, footers, side panels, forms, hidden elements, inline SVG, iframes and comments,
- changes relative `src` and `href` values to absolute URLs, so that images and links work outside the site,
- gives consistent output: ATX headings, `-` bullets, at most one blank line between blocks, no trailing whitespace.

The dashboard shows the result rendered (default) and as Markdown source (a toggle), and has a copy button.

## 2. Research: packages and services

### 2.1 Options

| Option | Kind | What it does | Notes |
|---|---|---|---|
| **ReverseMarkdown** 6.3.0 (`mysticmind/reversemarkdown-net`) | .NET library, MIT, about 8.2M downloads, released 2026-10-21 | HTML → Markdown. GitHub/CommonMark flavours, a `Preprocess` pipeline (`RemoveScripts`, `Remove(selector)`, `Unwrap`), comment removal, smart hrefs | v6 uses **AngleSharp** (≥ 1.5.2) instead of HtmlAgilityPack. Mature, maintained, net10.0 target |
| **SmartReader** 0.11.1 | .NET library, Apache-2.0, about 750K downloads | Port of Mozilla Readability: finds the main article and removes ads and side panels | Also AngleSharp. Still 0.x. Readability can drop content on pages that are not articles (home pages, listings, docs indexes). Gives cleaned HTML, not Markdown |
| Html2Markdown and other small .NET converters | .NET library | HTML → Markdown | Less used and less maintained than ReverseMarkdown |
| Our own HtmlAgilityPack walker | Our code | Visit the DOM and write Markdown | No new dependency. But Markdown escaping, nested lists, tables, `pre`/`code` and inline whitespace have many edge cases. That code is a project of its own |
| Trafilatura / markdownify (Python) | Python library | Best boilerplate removal in published comparisons | Needs a Python sidecar service and container. Too heavy here |
| Turndown + Readability (JS) | JS library | The same conversion in the browser | The conversion would be in the browser, so the backend (future embedding stage) could not use it. Breaks "one place" |
| Jina Reader (`r.jina.ai`), Firecrawl | Hosted API | URL in, Markdown out | They **fetch the URL again themselves**. Their result is not the copy we crawled (different time, rendering, robots handling). The crawled URLs go to a third party. They cost money, have rate limits, need a network allowlist and an API key, and bypass our SSRF/address policy. Rejected |

### 2.2 Decision: add one dependency, ReverseMarkdown

- The HTML-to-Markdown conversion has the most edge cases. A small, well-used, MIT library does it, and it is worth the dependency.
- Removing the boilerplate and changing the URLs needs no library. HtmlAgilityPack is already in `SamoBot.Adapters`. We do that ourselves in one small pass (section 4.2). Then the rules are ours, are tested, and are easy to change.
- SmartReader (Readability) is **not** added now. A simple rule ("use `<main>`, else the only `<article>`, else `<body>`, and remove `nav, aside, footer, …`") handles most pages and never drops a whole page. Because the converter sits behind a port, a Readability step can be added later inside the adapter. No caller changes (open/closed).
- Cost of the dependency: AngleSharp comes in as a transitive package of `SamoBot.Adapters`. It does not reach `SamoBot.Core`. The dependency rule of the repository still holds.

Sources: [ReverseMarkdown on NuGet](https://www.nuget.org/packages/ReverseMarkdown), [ReverseMarkdown README](https://github.com/mysticmind/reversemarkdown-net), [SmartReader on NuGet](https://www.nuget.org/packages/SmartReader), [HTML → Markdown for LLMs](https://tds.s-anand.net/2026-02/docs/week-6/html-to-markdown/), [Jina AI vs. Firecrawl](https://blog.apify.com/jina-ai-vs-firecrawl/).

## 3. Stored or on demand?

| | On demand (API converts when asked) | Stored (worker converts during parse and saves) |
|---|---|---|
| Schema / storage | None | A new object per fetch, or a new column; a migration; a backfill for pages already crawled |
| Converter improvements | Every page benefits at once | Old pages keep old output until a re-crawl or backfill |
| Cost | A few ms to tens of ms per view (HTML ≤ 2 MB) | Paid once for every page, including the many that nobody opens |
| `noarchive` policy | Uses the existing check | Must check the policy again on every read path |
| Bulk use (embeddings) | Each consumer converts. Fine for batch jobs | Read the stored result |

**Decision: on demand now.** The only consumer today is a person who opens one page, so storage, a migration and a backfill give no benefit yet.
The converter is a Core port (`IMarkdownConverter`) and a pure function. When an embedding stage arrives, the worker calls the same port in its own loop. That stage can then store the result if profiling shows a need. The API and the converter do not change. Write that down as the follow-up in section 7 and do nothing else now.

## 4. Design

### 4.1 Where the code lives (one place)

```
SamoBot.Core/
  Ports/IMarkdownConverter.cs          the port: string Convert(string html, Uri pageUrl)
  Domain/StoredCopy.cs                 + Url (the address the body was fetched from)
SamoBot.Adapters/
  Markdown/                            ← all the feature code is here
    ReverseMarkdownConverter.cs        public sealed; implements the port; runs the 3 steps
    HtmlCleaner.cs                     internal static; HtmlAgilityPack: pick the root, remove noise, make URLs absolute
    MarkdownTidy.cs                    internal static; normalises the Markdown text
  AdaptersServiceCollectionExtensions  + .AddSingleton<IMarkdownConverter, ReverseMarkdownConverter>()
SamoBot.Api/
  Controllers/PagesController.cs       + GET {id}/markdown (shares the load and policy code with {id}/content)
frontend/src/features/sites/
  pages-api.ts                         + usePageMarkdown
  StoredBodyState.tsx                  shared loading / hidden / error / truncated states (used by HTML and Markdown)
  MarkdownView.tsx                     Rendered | Source toggle, copy button
  MarkdownRender.tsx                   react-markdown + remark-gfm, safe links and images
  markdown-urls.ts                     pure helpers for allowed link and image URLs (unit tested)
  PageContentCard.tsx                  + "Markdown" tab
```

SOLID:
- **S:** the cleaner cleans, ReverseMarkdown converts, the tidy step normalises, the controller handles HTTP only, and the React components only show data.
- **O / D:** the controller (and later the worker) depends on `IMarkdownConverter` in Core, not on ReverseMarkdown. You can change the library or add Readability without changing any caller.
- **I:** the port has one method.
- **L:** the port is a pure function with no hidden state. Fakes in tests can replace it.

### 4.2 The conversion pipeline (`ReverseMarkdownConverter.Convert`)

1. **`HtmlCleaner.Clean(string html, Uri pageUrl) → string`** (HtmlAgilityPack, one parse):
   1. Base URL: use `<base href>` when it is present and absolute (or relative to `pageUrl`). Else use `pageUrl`.
   2. Root: the first `<main>`. Else the `<article>` when there is exactly one. Else `<body>`. Else the whole document.
   3. Remove these elements with their content: `script, style, noscript, template, svg, canvas, iframe, frame, object, embed, form, button, input, select, textarea, nav, aside, footer, dialog, link, meta`. Also remove any element with `hidden`, `aria-hidden="true"`, `role` in (`navigation`, `banner`, `contentinfo`, `complementary`, `search`, `dialog`), or an inline `style` that has `display:none`. Remove comments.
      Do **not** remove `<header>`: on many sites it holds the `<h1>` of the article, and `nav` inside it is removed anyway.
   4. Images: when `src` is missing, empty, or a `data:` URI, use the first of `data-src`, `data-lazy-src`, `data-original`. Remove the `<img>` when there is still no `http(s)` URL. Make `src` absolute. Remove `srcset`, `width`, `height`, `style` and `class`.
   5. Links: make `href` absolute. When the scheme is not `http`, `https` or `mailto`, or the href starts with `#`, unwrap the `<a>` (keep its text). Remove anchors that have no text and no image.
   6. Return the root's `OuterHtml`.
2. **ReverseMarkdown** with this configuration (check the exact property names against the 6.3.0 docs at https://mysticmind.github.io/reversemarkdown-net/; stop and report when one does not exist):
   - flavour **GitHub** (pipe tables, fenced code),
   - remove comments, smart href handling **on** (a link whose text equals its URL is written as `<url>`),
   - unknown tags: **bypass** (keep the content, drop the tag). This handles `div`, `span`, `section` and custom elements,
   - list bullet `-`,
   - base64 images: **skip**.
   Build the `Config` once (a static readonly field). Create a `new Converter(config)` **for each call**. The README says nothing about thread safety, and the adapter is a singleton.
3. **`MarkdownTidy.Normalize(string md) → string`**: change `\r\n` to `\n`, trim trailing whitespace on each line, collapse 3 or more newlines to 2, remove lines that hold only `[]()`, `![]()` or a lone `-`, trim the result, and end it with exactly one `\n` when it is not empty.

Input decoding: decode the stored bytes as UTF-8, the same as `HtmlAgilityPackParser` does today. Known limitation: other charsets are out of scope, as they are for the parser.

### 4.3 API contract

`GET /api/v1/pages/{id}/markdown`

| Case | Response |
|---|---|
| Page or stored body unknown | `404` |
| Policy forbids a stored copy (`noarchive`) | `403` problem details, same title as `/content` (`PagesController.NoArchiveMessage`) |
| OK | `200`, `Content-Type: text/markdown; charset=utf-8`; headers `X-Content-Truncated` (true when the **source HTML** was cut at 2 MB), `X-Content-Type-Options: nosniff`, `Content-Security-Policy: default-src 'none'; sandbox`, `Content-Disposition: inline; filename=page-{id}.md` |

The base URL for the relative references is the URL the body came from: `COALESCE(f."FinalUrl", u."Url")` of the same fetch row that `FindStoredCopy` already selects.

### 4.4 Frontend

- Dependencies: `react-markdown` (^10) and `remark-gfm` (^4). Do **not** add `rehype-raw`: raw HTML in the Markdown must never render, and react-markdown escapes it by default. Also set `skipHtml`.
- Links: open in a new tab with `rel="noopener noreferrer nofollow"`. Show only `http:`, `https:` and `mailto:` URLs; show other URLs as plain text (`safeHref` in `markdown-urls.ts`).
- Images: only `http(s)` (`safeImageSrc`), `loading="lazy"`, `referrerPolicy="no-referrer"`, `max-width: 100%`. They load from the original site. This is a privacy trade-off that a crawler dashboard accepts. `no-referrer` hides the dashboard origin.
- Styles: a scoped `.markdown-body` block in `src/index.css` with the existing design tokens: heading sizes, list indent, `pre` like `.well`, table borders, block quote rule. Do **not** add `@tailwindcss/typography`.
- The tab loads the data only when it is opened (Radix unmounts tabs that are not active, as it does for `SourceView` today).

## 5. Check commands

Backend (after each A task):

```bash
dotnet build SamoBot.sln -warnaserror
dotnet test src/SamoBot.Tests --filter "Category!=Integration"
```

Run this after A2 (it changes SQL; Docker is necessary):

```bash
dotnet test src/SamoBot.Tests --filter "Category=Integration"
```

Frontend (after each B task, in `src/frontend`):

```bash
npm run lint && npm run typecheck && npm test && npm run build
```

## 6. Tasks

### 6.1 Order and parallel work

```
T0 ──┬── A1 ── A2 ── A3 ── A4 ── A5          (backend, one agent)
     └── B1 ── B2 ── B3 ── B4                (frontend, one agent, uses the MSW mock until A4 is merged)
                                   └── C1    (docs and a manual end-to-end check, after both tracks)
```

### T0. Contract (do this first, small)

- Add the row for `GET /api/v1/pages/{id}/markdown` to `docs/api-contract.md`, under the `/content` row, with the text of section 4.3.
- Check: the file renders; no code changes.

### Track A: backend

**A1. Port and dependency.**
- Add `<PackageVersion Include="ReverseMarkdown" Version="6.3.0" />` to `Directory.Packages.props` (keep the alphabetical order) and `<PackageReference Include="ReverseMarkdown" />` to `src/SamoBot.Adapters/SamoBot.Adapters.csproj`.
- Create `src/SamoBot.Core/Ports/IMarkdownConverter.cs`:
  ```csharp
  namespace SamoBot.Core.Ports;

  /// <summary>Turns stored page HTML into clean Markdown: headings, text, lists, tables, links and images; no scripts, styles or navigation.
  /// Relative references are resolved against <paramref name="pageUrl"/>. Pure and thread-safe.</summary>
  public interface IMarkdownConverter
  {
      string Convert(string html, Uri pageUrl);
  }
  ```
- Check: build passes; `SamoBot.Core.csproj` has no new reference.

**A2. Return the URL of the stored copy.**
- `src/SamoBot.Core/Domain/StoredCopy.cs`: add `Uri Url` as the 2nd positional parameter: `StoredCopy(string ObjectName, Uri Url, PagePolicy Policy)`. Update the XML doc.
- `src/SamoBot.Adapters/Postgres/PageSql.cs` `StoredCopy`: join `"{TableNames.DiscoveredUrls}" u ON u."Id" = f."DiscoveredUrlId"` and select `COALESCE(f."FinalUrl", u."Url") AS "Url"`.
- `src/SamoBot.Adapters/Postgres/PostgresPageReader.cs` `FindStoredCopy`: read the new column and build the `Uri` with `new Uri(url, UriKind.Absolute)`.
- Fix the callers and fakes: `PagesControllerTests.FakePageReader`, and any other reference you find with `grep -rn "new StoredCopy(" src`.
- Tests: in `src/SamoBot.Tests/Postgres/PostgresReadModelTests.cs` (or the test file that already covers `FindStoredCopy`), add a test that `Url` is the final URL when it is set, and the page URL when it is not.
- Check: unit tests and integration tests.

**A3. The converter (the core of the feature).**
- Create `src/SamoBot.Adapters/Markdown/HtmlCleaner.cs`, `MarkdownTidy.cs` and `ReverseMarkdownConverter.cs` exactly as section 4.2 says. Reuse `HtmlDocumentExtensions.Nodes` from `Html/` where it fits.
- Keep each method at or below the limits of the repository's analyzers (`CodeMetricsConfig.txt`, `SonarLint.xml`). Split helpers rather than suppress warnings.
- Tests: `src/SamoBot.Tests/Markdown/ReverseMarkdownConverterTests.cs` (one `[Fact]` per rule, small inline HTML):
  1. `h1`–`h6` become `#`–`######`.
  2. `script`, `style`, `noscript`, `svg`, `iframe`, `form` and comments are gone (assert that their text is not in the output).
  3. `nav`, `aside`, `footer` and `[role=navigation]` are gone. The `h1` inside `<header>` stays.
  4. With `<main>`, text outside `main` is gone. With two `<article>`s, the whole body is kept.
  5. `<img src="/a.png" alt="A">` on `https://ex.com/x/` → `![A](https://ex.com/a.png)`. Relative `href` → absolute. `<base href>` is used.
  6. Lazy image `data-src` is used. A `data:` image and an image without a URL are removed.
  7. A `javascript:` link and a `#top` link become plain text.
  8. Lists, nested lists, a table (GitHub pipe table), `pre > code` (fenced) and `blockquote` convert.
  9. HTML entities are decoded (`&amp;` → `&`), and Markdown special characters in the text are escaped (`1. not a list` in a `<p>`).
  10. Output has no `\n\n\n`, no trailing spaces, and ends with one `\n`. Empty input or a body with only scripts gives `""`.
  11. Hidden elements (`hidden`, `aria-hidden="true"`, `style="display:none"`) are gone.
  Also add `src/SamoBot.Tests/Markdown/MarkdownTidyTests.cs` for the normalisation rules.
- Check: unit tests.

**A4. Registration and endpoint.**
- `AdaptersServiceCollectionExtensions.AddSamoBotAdapters`: add `.AddSingleton<IMarkdownConverter, ReverseMarkdownConverter>()` next to `IHtmlParser`.
- `ServiceRegistrationTests`: assert that `IMarkdownConverter` resolves.
- `PagesController`: inject `IMarkdownConverter`. Move the shared part of `GetContent` (find the copy, return 404 or 403, load and cap the bytes) to one private method that returns either an `IActionResult` error or `(StoredContent content, StoredCopy copy)`. Use it in both actions.
  Add `GetMarkdown` at `[HttpGet("{id:int}/markdown")]`: decode UTF-8 → `converter.Convert(html, copy.Url)` → `File(Encoding.UTF8.GetBytes(md), "text/markdown; charset=utf-8")`. Generalise `SetContentHeaders(id, truncated)` to take the file name (`page-{id}.txt` or `page-{id}.md`). Keep the `[ProducesResponseType]` attributes in the same form as `GetContent`.
- Tests in `PagesControllerTests`: 404 (no copy), 403 (noarchive), 200 with `text/markdown` + all four headers + `X-Content-Truncated: true` for a body over `StoredContent.MaxBytes`; the converter receives `copy.Url`. Use a small `FakeMarkdownConverter` that records its arguments. Put it in `src/SamoBot.Tests/Fakes/`. The existing `/content` tests must still pass unchanged.
- Check: unit tests; `ApiJsonContractTests` still pass.

**A5. Performance guard.**
- Add one test that converts a generated HTML of about 2 MB (for example 20 000 paragraphs with links and images) and asserts that it finishes in under 2 s. Mark it `[Trait("Category", "Integration")]` so that it does not make the fast suite slow or flaky.
- Check: integration tests.

### Track B: frontend

**B1. Data hook and mock.**
- `npm install react-markdown@^10 remark-gfm@^4` in `src/frontend` (commit `package.json` and `package-lock.json`).
- `pages-api.ts`: replace `fetchContent` with `fetchStoredText(path, accept)` that returns `{ text, truncated }`. Keep `usePageContent` (same query key `['page-content', id]`) and add `usePageMarkdown(id, enabled)` with key `['page-markdown', id]`, `Accept: text/markdown`, and `retry: retryUnlessMissing`.
- `src/mocks/handlers/pages.ts`: add `*/api/v1/pages/:id/markdown`. It returns a fixed sample Markdown with headings, a list, a table, an image (`https://picsum.photos/…` or a local `/vite.svg`) and a link; 404 when `!page?.parsed`, the same as `/content`.
- Check: frontend checks.

**B2. Safe URL helpers.**
- `markdown-urls.ts`: `safeHref(url?: string): string | undefined` (only `http:`, `https:`, `mailto:`) and `safeImageSrc(url?: string): string | undefined` (only `http:`, `https:`). Use `new URL()` in try/catch; relative or invalid input gives `undefined`.
- `markdown-urls.test.ts`: allowed schemes, `javascript:`, `data:`, `vbscript:`, mixed case `JaVaScRiPt:`, relative, empty, undefined.
- Check: frontend checks.

**B3. Components.**
- `StoredBodyState.tsx`: move the pending, forbidden, error and truncated note logic of `SourceView` here as one component, `StoredBodyState({ query, children(data) })`, or as a small set of components. Change `SourceView` to use it. Its look and text stay the same.
- `MarkdownRender.tsx`: `<ReactMarkdown remarkPlugins={[remarkGfm]} skipHtml components={{ a, img }}>` inside `<div className="markdown-body max-w-[72ch]">`. `a` uses `safeHref` (when it gives `undefined`, render a `<span>`). `img` uses `safeImageSrc` (when it gives `undefined`, render nothing), with `loading="lazy"`, `referrerPolicy="no-referrer"` and `alt`.
- `MarkdownView.tsx`: uses `usePageMarkdown(id, true)` and `StoredBodyState`; a small two-option toggle **Rendered | Markdown source** (reuse `Tabs` from `components/ui/tabs`, or two `Button`s with `aria-pressed`); a **Copy** button (`navigator.clipboard.writeText`, toast through `sonner` on success or failure); source mode shows `<pre className="well m-0 max-h-[600px]">`. Empty Markdown → `EmptyState` "No readable content".
- `index.css`: add the `.markdown-body` styles (section 4.4).
- Check: frontend checks.

**B4. The tab.**
- `PageContentCard.tsx`: `type View = 'text' | 'markdown' | 'html' | 'headings' | 'links'`; add `<TabsTrigger value="markdown">Markdown</TabsTrigger>` after **Text**. In `ViewBody`, before the `!page.parsed` check, add `if (view === 'markdown') return <MarkdownView id={page.id} />` (it depends on the stored body, not on the parse result, like `html`).
- Check: frontend checks; `npm run dev` with MSW, open a page, the Markdown tab shows the rendered sample, the toggle and copy work, and keyboard tab navigation works.

### C1. Finish (after A and B)

- `README.md`: add one sentence under the dashboard text: the page screen has a Markdown tab, made on demand by `GET /api/v1/pages/{id}/markdown`.
- Manual end-to-end check with `docker compose up --build`: crawl a news article, a docs page and a home page. For each page, open the Markdown tab. Confirm that there is no nav or footer noise, that the headings and images are there, and that a `noarchive` page shows "Stored copy hidden". Note the results in the PR description.

## 7. Out of scope (follow-ups)

- **Embeddings / LLM stage:** a new worker loop (`IWorkHandler`) that loads the stored body, calls `IMarkdownConverter`, splits the Markdown into chunks at headings, and embeds the chunks. Decide then whether to store the Markdown (as an S3 object next to the HTML, `ObjectNameGenerator` + `.md`).
- **MCP tool** `get_page_markdown(url)` in `SamoBot.Mcp`, so that AI tools can read the clean content.
- **Readability step** (SmartReader) inside `ReverseMarkdownConverter`, when the simple root rule gives too much noise on real pages.
- Charset detection other than UTF-8 (shared with the parser).
