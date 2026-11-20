# 0015. Dashboard frontend

Status: Accepted

## Decision

**Accessibility.** Colour is never the only signal. An HTTP status shows a tone, a word and a text mark (`httpStatusLook`). A
status LED is only a mark, so a word always goes next to it. The badge tones are always paired with a word, and with a dot where
they mean "live". The response bar's legend repeats every number in words. The stat counters announce changes politely. The
request log is *not* a live region, because it would read out every request.

**State in the URL.** Filters, search (`?q=&host=&page=`), analytics range and host, crawl group (`?show=`) and content view
(`?view=`) live in the URL, so views can be shared and reloaded.

**Live crawl data (`useCrawlData`, `useEventSync`, `crawl-state`, `crawl-hub`).**
- The screen loads `/progress` and `/events` once, then takes live events from SignalR. When the socket is down, it polls.
- It syncs every 3 s while the socket is not live and every 30 s while it is, and once on every transition into live (first
  connect, reconnect, recovery from offline).
- An event is applied once. The newest 500 applied ids are kept, so a live event and a poll of the same event count once.
  A live event without an id is always applied. The largest id applied from any source is the cursor of the next poll.
- With no cursor, the events endpoint returns the newest page (tail mode). Otherwise the client pages on while a page is full,
  up to a maximum number of pages.
- A finished request moves the counters on at once, and the next snapshot replaces them.
- The job is read again while links still move, because parsing and indexing go on after the last fetch.
- `start()` is never called on a connected hub. A connected hub only joins the group again.
- The hub needs the CSRF header on every cookie POST, the negotiate request included. A 401 from negotiate exists only inside the
  error message, so `isUnauthorized` reads the message.
- The mock stream is loaded on demand, so it stays out of the normal bundle.

**Session (`AuthProvider`, `lib/api`).** Any 401 from the API means the session is gone. The user is dropped and `ProtectedRoute`
redirects. `notifyUnauthorized` covers callers outside `apiFetch`, such as the hub. On sign-out, every cached query except
`/me` is dropped, because `/me` is still observed. The crawl jobs query is shared by the crawl screens and the sidebar, so they
use one cache entry. `retryUnlessMissing` retries transient errors but never a 404 or a 403.

**Untrusted content.**
- Snippets contain `<mark>` tags only. They are rendered by splitting on the literal tags, never through `innerHTML`.
  `decodeEntities` decodes exactly five entities in one pass, so `&amp;lt;` becomes `&lt;` and not `<`.
- Stored HTML is shown as text with line numbers and colours, and never runs as HTML. Above `HIGHLIGHT_LINE_LIMIT` lines,
  the colours are dropped to keep the page fast.
- Markdown is rendered as React elements. Raw HTML is dropped. Links may use http, https or mailto, and images only http or
  https. YAML front matter is metadata and is not rendered. External links are offered for http and https only.

**Robots facts.** The server stays the source of truth for the crawl delay. The client reads only the other facts from the file text.

**Theme.** Light or dark mode is the `dark` class on `<html>`, remembered per browser. `index.html` applies it before paint. When
storage is blocked, the choice lasts for that page only.

**Styling.** Every colour is a CSS variable that holds space-separated RGB channels (`src/index.css`), so Tailwind opacity
modifiers such as `bg-primary/80` keep working.

**Google sign-in.** `google.ts` holds minimal hand-written types for Google Identity Services instead of a types package.

**Build and dev server (`vite.config.ts`).**
- The MSW worker is copied from `public/` into `dist/`. Production never registers it, so the build drops it unless mocks are on.
- For local development without Google sign-in, a set `DEV_API_KEY` makes the dev server forward `/api` and `/hubs` to
  `DEV_API_TARGET` with the `X-Api-Key` header. The key stays in the dev server: it has no `VITE_` prefix, so it never reaches the
  browser or the build. Leave `VITE_API_BASE` empty so the app calls the dev server.

**Mocks.** Mock data comes from the design files (`docs/design/*.dc.html`): Live, Site, Main, Analytics and Search. `docs.example.com` is
crawled live by job 42. `window.__crawlMock.drop(8000)` simulates an 8-second connection drop to test the resync by hand.
