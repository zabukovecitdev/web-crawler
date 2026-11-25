# 0002. Network safety (SSRF)

Status: Accepted

## Context

The crawler fetches URLs that users and web pages supply. Without a check, a crawl could reach loopback, the private network
or a cloud metadata service. The section "Network safety (SSRF)" of [architecture.md](../architecture.md#network-safety-ssrf) gives an overview. These are the decisions behind it.

## Decision

**Address policy (`IpAddressPolicy`).** The address policy is pure: it does no I/O. It denies private, loopback,
link-local (cloud metadata), CGNAT, multicast, reserved and documentation ranges. IPv6 forms that carry an IPv4 address
(mapped, compatible, NAT64, 6to4, Teredo) are judged by the IPv4 address they carry.

**Check at connect time (`SafeConnect`, `SafeHttpClientExtensions.AddSafeHttpClient`).**
- `SafeConnect` is the `SocketsHttpHandler.ConnectCallback` of every client that fetches from the open web (fetcher, robots,
  sitemaps). It takes the allowed addresses of the host and connects to the first one that works.
- Redirect hops open their own connection, so the policy checks each hop. The fetcher does not follow redirects, because the
  frontier handles them. The robots source follows a few.
- The callback only supplies the stream. The request URL, the Host header and the TLS server name stay those of the original
  request, although the connection goes to an IP address.
- When every cached address fails, the callback resolves once more, updates the cache and tries the new addresses.

**Resolver and cache (`SafeResolver`, `MemoryHostAddressCache`, `PostgresHostAddressCache`).**
- Cached addresses younger than `Crawler:DnsCacheMinutes` are used before DNS. Every address is checked against the policy
  on every use, so a cache entry never bypasses the check. IP literals get the same check and are never cached.
- `AllowedAddresses.FromCache` tells the caller that a failed connect may retry with fresh DNS.
- `Refresh` ignores the cache and stores the allowed addresses. When no address is allowed, it clears the cache and throws
  `BlockedAddressException`.
- The cache never decides freshness. The caller checks `ResolvedHost.ResolvedAt`. `IHostAddressCache` only remembers: the
  caller checks every address again before it uses it.
- `MemoryHostAddressCache` sits in front of PostgreSQL, so a connect does not query the database each time. It remembers a
  name that the inner cache did not know as unknown until that name is saved.
- `PostgresHostAddressCache` keys on the DNS name. A Hosts row (whose key is the authority, with or without a port) belongs to
  the name when the part before the port is that name. A name with no Hosts row is not stored.

**Browser (`BrowserRequestGuard`, `BrowserFetcher.ContextOptions`).**
- Every request of a browser context (navigation, redirect hops, sub-requests, popups) is checked against the policy. A
  blocked request is aborted and every WebSocket is refused.
- Only `data:` and `blob:` pass without a network check. Every other non-http scheme (`file:`, `ftp:`, `ws:`, ...) is refused.
- A host that our resolver cannot resolve is refused, but the page is not marked blocked.
- The browser picks the address itself, so `SafeResolver.AllAllowed` requires every address of the host to be allowed, from
  DNS only, without the cache.
- Service workers are off, because they would answer requests the guard never sees. Downloads are off, because a crawler
  has no use for them.
- Chrome resolves the name again when it connects, so a short DNS-rebinding window remains. A worker may also open a socket
  that the route does not see. The network policy of the Chrome container (no route to private ranges) is the control for both.

**Settings (`CrawlerNetworkOptions`).**
- `AllowPrivateAddresses` is for development and tests only. The worker and the API log a warning at startup when it is on.
- `UseProxy` defaults to false. Through a proxy the connect check would see the proxy address instead of the target, and the
  check would no longer protect anything.

**Seeds and job hosts (`SeedUrlValidator`, `HostRules`).**
- Seed URLs are checked before anything is stored. Only absolute http and https URLs pass, with no credentials, and not
  pointing at a local, private or metadata address. This checks the URL text only. The fetcher still checks the resolved addresses.
- A host name without a dot is rejected: only a local resolver knows a single-label name.

**Blocked hosts (`CrawlOutcome.Blocked`).** A host that resolves only to disallowed addresses is skipped for good.

## Consequences

Every new HTTP client that fetches from the open web must be added with `AddSafeHttpClient`.
