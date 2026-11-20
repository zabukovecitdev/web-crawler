# 0009. API security

Status: Accepted

## Decision

**Authentication (`AuthSchemes.Smart`, `ApiKeyAuthenticationHandler`, `ApiKeyOptionsValidator`).**
- One policy scheme picks the API key when `X-Api-Key` is present, otherwise the cookie.
- Keys are compared as SHA-256 digests in fixed time, so neither the content nor the length leaks through timing.
- In Production a configured API key must be long and must not be a well-known placeholder. An empty key is valid and disables
  the API key.

**Who may sign in (`AuthOptions`, `EmailAllowlist`, `SessionValidation`, `SessionPrincipal`).**
- When both allow lists are empty, nobody may sign in (fail closed).
- The lists follow configuration reloads, so removing an entry also affects sessions that are already signed in.
  `SessionValidation` checks every cookie-authenticated request against the allowlist with an in-memory lookup.
- The Google hosted domain (`hd`) is kept as a claim so the allowlist can be checked again on every request.
- A session ends `MaxAge` (24 h) after sign-in, however often it is used. Sliding renewal keeps `auth_time`, so that claim marks
  the absolute start.

**Cookie (`AuthOptions.AllowInsecureCookie`).** The cookie is `Secure` unless the environment is Development or
`Auth:AllowInsecureCookie` is set (compose on localhost). Behind HTTPS, leave the setting false.

**Pipeline order (`UseSamoBotAuthentication`, `UseSamoBotAuthorization`).** Authentication runs, then rate limiting, then the
origin and CSRF checks, then the `[Authorize]` decision. Rate limiting then knows the caller and still counts the 401s.

**CSRF (`CsrfHeaderMiddleware`).** Cookie sessions need CSRF protection on state-changing calls. A cross-site form cannot set a
custom header. A cross-site fetch with one triggers a CORS preflight, which only allowed origins pass. Requests authenticated
with the API key (not a cookie) are exempt.

**Origin check (`OriginCheckMiddleware`).** WebSockets are not subject to CORS, and a page on a sibling subdomain sends the
session cookie (SameSite=Lax is per site, not per origin). So the hub and the sign-in call accept a browser only when its
`Origin` is the API's own origin or a configured CORS origin. Requests without `Origin` (not a browser) pass.

**CORS (`DashboardCorsOptions`).** An empty list means no cross-origin caller. The development list lives in
`appsettings.Development.json` so that it never merges into an environment override. Array configuration merges by index.

**Reverse proxy (`ForwardedHeadersSettings`).** Behind a proxy, the peer address is the proxy's, so every client would share one
rate-limit bucket. `Request.Scheme` would also be http for an https browser, and the origin check compares the scheme. Listing
the proxy makes the API read `X-Forwarded-For` and `X-Forwarded-Proto`. Nothing is trusted by default, so a header from any other
peer is ignored and cannot be spoofed.

**Rate limiting (`RateLimitingExtensions`).**
- This is the only rate limiter: the dashboard's nginx does not limit.
- It keys on the signed-in user, otherwise on the client address. An IPv6 client is limited as a whole /64, because one host owns that many addresses.
- A wrong `X-Api-Key` is a guess, so it gets far fewer attempts (`ApiKeyFailuresPerMinute`). Sign-in has its own limit on top
  of the anonymous one.
- All limiters are token buckets: a short burst is allowed, then requests are admitted at a steady rate.
- The hub is limited like any path. That counts only the HTTP requests that open a connection (negotiate, upgrade), not the
  messages inside it.

**Stored bodies (`PagesController.SetContentHeaders`).** A stored body is untrusted page content. Responses forbid sniffing and
any active use, in case a browser opens them directly. A page with `noarchive` gives 403.
