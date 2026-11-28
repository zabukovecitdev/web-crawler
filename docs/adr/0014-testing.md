# 0014. Testing

Status: Accepted

## Decision

- Tests that need Docker (PostgreSQL, RustFS, Meilisearch) or real timing are `Category=Integration`. CI runs them in a step of
  their own.
- Without Docker, the PostgreSQL integration tests can run against a local, empty database: set `SAMOBOT_TEST_POSTGRES` to
  its connection string. The fixture then runs the real migrations there instead of starting a container. Migration tests create and
  drop a scratch database of their own, so that role needs `CREATEDB`. Tests that also need
  RustFS or Meilisearch still need Docker.
- The end-to-end test (`EndToEnd/CrawlJobTests`) crawls a stub site with real PostgreSQL, RustFS and Meilisearch, but with a fake
  clock, so the retry backoff of the failing page costs no real time. When work was done, the clock moves just past the host
  delay. When idle, it jumps to the next retry.
- The stub site runs on loopback, which the seed validator rejects, so the test inserts the job straight into the repository.
- `TestServer` has no peer address. Auth tests set one with a test header to play a client or a proxy.
- Rate-limit tests depend on the burst of a token bucket: `max(3, permitsPerMinute / 5)`. With small limits, the fourth call is
  the first one refused.
- Session tests keep using the session, because the cookie slides (8 h) while the 24 h limit counts from sign-in.
- `ReverseMarkdownConverterPerformanceTests` guards that a page at the size cap converts fast enough to convert on demand.
  It is timing-based, so it is an integration test. The fastest of three runs counts: a slow machine or a GC pause hits one
  run, a slow algorithm hits all of them. The first budget was 2 s. On a 4-core CI-like machine that also runs the Docker tests, the
  fastest run is about 1.2 s alone and 2.1 s under that load, so the budget is 3 s.
