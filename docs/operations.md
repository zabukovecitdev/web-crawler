# Operations

How to run, configure and secure SamoBot. The design behind it is in [architecture.md](architecture.md), and the reasons are
in the [ADRs](adr/README.md).

## Run with Docker Compose

```bash
cp .env.example .env
openssl rand -hex 32   # put the output in .env as DASHBOARD_API_KEY
docker compose up --build
```

This starts PostgreSQL, RustFS (S3 storage, console at http://localhost:9001/rustfs/console/), Meilisearch, the migrations, the
worker, the API (port 5080) and the dashboard (http://localhost:8081).

| Service (`compose.yaml`) | Published on | Notes |
|---|---|---|
| `frontend` | `127.0.0.1:8081` | nginx with the built dashboard, forwards `/api` and `/hubs` to the API |
| `api` | `127.0.0.1:5080` | REST under `/api/v1`, SignalR hub `/hubs/crawl-job` |
| `postgres` | `127.0.0.1:5432` | PostgreSQL 16, database, user and default password `samobot` |
| `rustfs` | `127.0.0.1:9000`, `127.0.0.1:9001` | S3 API and console |
| `meilisearch` | `127.0.0.1:7700` | Meilisearch v1.11 |
| `migrations` | | Runs once; the worker and the API start after it succeeds |
| `worker` | | The crawl, parse, index, job completion and sitemap loops |
| `chrome` | | Only with the profile `js` (see [JavaScript rendering](#javascript-rendering)) |

Scripts send the key in the header `X-Api-Key`. The [MCP server](../src/SamoBot.Mcp/README.md) does not call the API: it reads
Meilisearch directly and needs no API key. The dashboard is in `src/frontend/`. The page screen has a
Markdown tab: `GET /api/v1/pages/{id}/markdown` turns the stored HTML into clean Markdown (headings, text, lists, tables, links,
images) on demand, for reading or as input for an LLM.

## Environment variables

Compose reads `.env` next to `compose.yaml` (copy `.env.example`).

| Variable | Default | Sets |
|---|---|---|
| `DASHBOARD_API_KEY` | none, required | `DashboardApi:Key` of the API (see [API key](#api-key)) |
| `POSTGRES_PASSWORD` | `samobot` | The PostgreSQL password and the connection strings. Avoid `;`, `=` and spaces in it. |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | `minioadmin` | RustFS credentials and `MinIO:AccessKey`, `MinIO:SecretKey` |
| `MEILI_MASTER_KEY` | `masterKey` | Meilisearch master key and `Meilisearch:ApiKey` |
| `GOOGLE_CLIENT_ID` | empty | `Google:ClientId`, and the dashboard build (see [Google sign-in](#dashboard-sign-in-google)) |
| `AUTH_ALLOWED_EMAILS`, `AUTH_ALLOWED_DOMAINS` | empty | `Auth:AllowedEmailsCsv`, `Auth:AllowedDomainsCsv` |
| `CORS_ORIGIN` | `http://localhost:5173` | `Cors:Origins:0` |
| `AUTH_ALLOW_INSECURE_COOKIE` | `true` | `Auth:AllowInsecureCookie` (see [Cookie](#cookie)) |
| `CHROME_RENDERING_ENABLED` | `false` | `ChromeRendering:Enabled` of the worker |

## Security settings

### API key

`DASHBOARD_API_KEY` is required. In Production (the Docker images) the API refuses to start when the key is shorter than 32
characters or a placeholder such as `change-me`; an existing deployment with a short key must be changed.

### Ports

Compose binds every published port (PostgreSQL, RustFS, Meilisearch, the API) to `127.0.0.1`. The database, storage and search
credentials default to development values; set `POSTGRES_PASSWORD`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` and `MEILI_MASTER_KEY` in
`.env` before publishing on another interface, and then only behind a firewall.

### Cookie

Compose serves plain http on localhost, so `AUTH_ALLOW_INSECURE_COOKIE` defaults to `true` (the session cookie is sent without
`Secure`). Set it to `false` behind HTTPS. Outside Development without this flag the cookie is always `Secure`.

### Allowlist changes

The allowlist is checked on every request, so removing an account signs it out on its next call. `appsettings.json` changes are
picked up at once; environment variables (compose) are read once, so restart the `api` container after editing
`AUTH_ALLOWED_EMAILS` or `AUTH_ALLOWED_DOMAINS`. A session also ends 24 hours after sign-in.

### Rate limits

Token buckets in the API (`Hosting/RateLimitingExtensions.cs`, section `RateLimiting`), answering `429` with `Retry-After`.

| Setting (`RateLimiting:`) | Default | Applies to |
|---|---|---|
| `UserPermitsPerMinute` | 300 | Each signed-in user. The deprecated `PermitsPerMinute` is still read when this one is not set, and logs a warning at startup. |
| `AnonymousPermitsPerMinute` | 120 | Each anonymous client address |
| `SignInPermitsPerMinute` | 10 | `POST /api/v1/auth/google` |
| `ApiKeyFailuresPerMinute` | 10 | An `X-Api-Key` that does not match |

A bucket holds a burst (a fifth of the per-minute rate, at least 3) and refills steadily. The limiter runs after authentication
and before authorization, so unauthenticated calls to protected endpoints are counted too. The hub is limited like any path;
that counts the requests that open a connection, not the messages inside it.

The API reads the client address and scheme from `X-Forwarded-For` and `X-Forwarded-Proto` only from the networks in
`ForwardedHeaders:KnownProxies` and `ForwardedHeaders:KnownNetworks` (compose trusts the private ranges); otherwise every client
behind a proxy would share one bucket. The dashboard's nginx does the same one step earlier: it takes the client from
`X-Forwarded-For` when the proxy in front of it is on a private network (`set_real_ip_from` in `nginx.conf`; add the ranges of a
CDN or a cloud load balancer there and in `ForwardedHeaders`) and sends the address and scheme on. Never publish the dashboard or
API port to the internet directly, since a direct client could forge these headers.

Nothing in nginx limits requests or connections: this limiter is the only one, and it does not cap concurrent hub connections.
Counters are in memory, so limits are per API instance.

## Dashboard

### Dashboard container

nginx serves the built dashboard and forwards `/api` and `/hubs` to the API, so the browser sees one origin and needs no CORS.
Vite builds `GOOGLE_CLIENT_ID` into the bundle: rebuild the image (`docker compose up --build frontend`) after you change it, and
add `http://localhost:8081` to the authorised JavaScript origins of the Google client. The container has no API-key sign-in; use
the Vite dev server for that (see [Dashboard without Google sign-in](#dashboard-without-google-sign-in-local-development)).

### Dashboard in dev

The API listens on http://localhost:5080 with both `dotnet run` and compose. With `dotnet run` it allows the origins in
`appsettings.Development.json`; with compose it allows `CORS_ORIGIN`.

### Dashboard sign-in (Google)

The dashboard signs in with Google and the API issues a cookie session (`samobot.session`: HttpOnly, SameSite=Lax, 8 hours
sliding and at most 24 hours from sign-in, Secure outside Development unless `AUTH_ALLOW_INSECURE_COOKIE=true`).
Set these in `.env`:

| Variable | Meaning |
| --- | --- |
| `GOOGLE_CLIENT_ID` | OAuth client ID (Web application). The dashboard origin must be an authorised JavaScript origin. |
| `AUTH_ALLOWED_EMAILS` | Comma-separated emails that may sign in. |
| `AUTH_ALLOWED_DOMAINS` | Comma-separated domains (matched against the verified `hd` claim or the email domain). |
| `CORS_ORIGIN` | Dashboard origin, default `http://localhost:5173`. |

Both lists empty means nobody can sign in (the API logs a warning at startup; the API key still works). Config keys:
`Google:ClientId`, `Auth:AllowedEmails`, `Auth:AllowedDomains`.
`http://localhost:5173` talking to `http://localhost:5080` is same-site (ports do not matter), so the Lax cookie works in dev over
plain http.
Frontend contract: `fetch(..., { credentials: 'include' })`; send `X-Requested-With: samobot` on every POST/PUT/PATCH/DELETE
(cookie sessions only; `/auth/google` is exempt); SignalR uses the same cookie. Unauthenticated calls return 401, never a redirect.

### Dashboard without Google sign-in (local development)

Until a Google client ID is set, the Vite dev server can sign every request in with the API key. The key stays in the dev server
(no `VITE_` prefix), so it is never sent to the browser or built into `dist/`. Do not use this outside your own machine: anyone
who can open the dev server acts as the API key user.

```bash
docker compose up --build                 # API on http://localhost:5080
cd src/frontend
cp .env.example .env.local
# in .env.local: VITE_API_BASE=   (empty)   DEV_API_TARGET=http://localhost:5080   DEV_API_KEY=<DASHBOARD_API_KEY from ../../.env>
npm ci && npm run dev                     # http://localhost:5173
```

The dashboard then shows the user "Dashboard". Signing out only shows the sign-in page; reload to come back.
Once `GOOGLE_CLIENT_ID` and an allowlist are set, remove `DEV_API_KEY` and set `VITE_API_BASE=http://localhost:5080`.

## JavaScript rendering

Pages that need JavaScript can be rendered with headless Chrome. It is off by default:

```bash
CHROME_RENDERING_ENABLED=true docker compose --profile js up --build
```

The `chrome` service sits on two networks of its own: `browser` (internal, shared only with the worker) and `chrome_egress` (a
bridge with the fixed subnet `172.31.250.0/24`). It is not on the default network, so Chrome cannot reach PostgreSQL, RustFS,
Meilisearch or the API. The worker reaches it at `http://172.31.251.10:9222` (an IP, because the DevTools endpoint rejects other
`Host` headers).

Chrome resolves host names itself, so the address checks of the crawler cannot close a DNS rebinding window completely. The
network is the control: drop new connections from the egress subnet to private and special ranges on the Docker host.

```bash
iptables -I DOCKER-USER -s 172.31.250.0/24 -m conntrack --ctstate NEW \
  -d 10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,169.254.0.0/16,100.64.0.0/10,127.0.0.0/8 -j DROP
```

On a cloud deployment use the provider's egress controls (security group or network policy) for the same ranges, including the
metadata address 169.254.169.254. This setup has not been verified end to end here (no Docker available); run
`docker compose --profile js up` once and crawl a JavaScript page before relying on it.

## Swagger

Swagger is served only in Development (`dotnet run`).

## Tests

```bash
dotnet build SamoBot.sln -warnaserror
dotnet test src/SamoBot.Tests --filter "Category!=Integration"
dotnet test src/SamoBot.Tests --filter "Category=Integration"   # needs Docker
```

Mutation testing is described in [mutation-testing.md](mutation-testing.md).
