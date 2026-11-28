#!/usr/bin/env bash
# Checks a running stack (docker compose up) end to end: API auth, a real crawl of one public page,
# the stored page, its Markdown, search, and the dashboard with its /api proxy.
#   DASHBOARD_API_KEY=... scripts/smoke-test.sh
# SMOKE_URL (default https://example.com/) is the page to crawl; SMOKE_QUERY a word it contains.
set -euo pipefail

API="${API:-http://127.0.0.1:5080}"
DASHBOARD="${DASHBOARD:-http://127.0.0.1:8081}"
URL="${SMOKE_URL:-https://example.com/}"
QUERY="${SMOKE_QUERY:-Example}"
KEY="${DASHBOARD_API_KEY:?Set DASHBOARD_API_KEY}"

pass() { printf '  PASS %s\n' "$1"; }
fail() { printf '  FAIL %s\n' "$1"; exit 1; }
api() { curl -fsS -H "X-Api-Key: $KEY" "$API$1"; }

wait_for() {
    local description="$1" seconds="$2"
    shift 2
    for _ in $(seq "$seconds"); do
        if "$@" > /dev/null 2>&1; then pass "$description"; return; fi
        sleep 1
    done
    fail "$description (gave up after ${seconds}s)"
}

job_status() { api "/api/v1/crawl-jobs/$1/progress" | jq -r .status; }
job_completed() { [ "$(job_status "$1")" = "Completed" ]; }
search_hit() { [ "$(api "/api/v1/search?q=$QUERY" | jq '.total')" -ge 1 ]; }

echo "API"
wait_for "API answers with the API key" 180 api /api/v1/crawl-jobs
status=$(curl -s -o /dev/null -w '%{http_code}' "$API/api/v1/crawl-jobs")
[ "$status" = 401 ] && pass "API refuses a call without a key (401)" || fail "API without a key answered $status, expected 401"

echo "Crawl $URL"
job=$(curl -fsS -H "X-Api-Key: $KEY" -H 'Content-Type: application/json' \
    -d "{\"seedUrls\":[\"$URL\"],\"maxDepth\":0,\"maxUrls\":1,\"startImmediately\":true}" \
    "$API/api/v1/crawl-jobs" | jq -r .id)
[ -n "$job" ] && [ "$job" != null ] && pass "crawl job $job created and started" || fail "crawl job not created"
wait_for "job $job completes" 300 job_completed "$job"
crawled=$(api "/api/v1/crawl-jobs/$job/progress" | jq .crawled)
[ "$crawled" -ge 1 ] && pass "job $job crawled $crawled page(s)" || fail "job $job completed with 0 pages crawled"

echo "Search and stored page"
wait_for "search finds \"$QUERY\"" 120 search_hit
page=$(api "/api/v1/search?q=$QUERY" | jq -r '.items[0].pageId')
api "/api/v1/pages/$page" | jq -e '.parsed.title | length > 0' > /dev/null && pass "page $page has a parsed title" || fail "page $page has no parsed title"
api "/api/v1/pages/$page/markdown" | grep -qi "$QUERY" && pass "page $page converts to Markdown" || fail "page $page Markdown lacks \"$QUERY\""

echo "Dashboard"
curl -fsS "$DASHBOARD/" | grep -qi '<html' && pass "dashboard serves the app" || fail "dashboard did not serve HTML"
curl -fsS -H "X-Api-Key: $KEY" "$DASHBOARD/api/v1/crawl-jobs" > /dev/null && pass "dashboard proxies /api to the API" || fail "dashboard /api proxy failed"

echo "All checks passed."
