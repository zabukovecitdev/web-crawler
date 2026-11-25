# SamoBot.Mcp

An MCP server that lets an AI assistant search the pages SamoBot has crawled.

It speaks MCP over stdio (`WithStdioServerTransport`) and has one tool, `search_pages`. It reads the Meilisearch index `pages`
directly through `ISearchQuery`, registered by `AddSamoBotSearch` from `SamoBot.Adapters`. It does not call the SamoBot API, so it
needs no `X-Api-Key` and no dashboard session. It sees only what the worker has indexed. All logs go to stderr, because stdout
carries the MCP messages (ADR 0013).

## Tool: `search_pages`

`SearchTools.SearchPages`. The description sent to the client is "Searches indexed pages and returns the title and canonical URL
of each match."

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `query` | string | required | The search query |
| `limit` | integer | 20 | Maximum number of results |
| `offset` | integer | 0 | Number of results to skip, for paging |

It returns a list of `PageMatch` records, in Meilisearch's relevance order. Each holds:

- `Title`: the page title.
- `Canonical`: the canonical URL that the page declared. It is empty when the page declared none. The page's own URL is not
  returned.

How the search works:

- Meilisearch matches the query against the title, description, keywords and body text of each page.
- The index holds one document per URL, and only pages whose policy allows indexing (ADR 0007). A page past its
  `unavailable_after` date is filtered out (`indexUntil` is in the past).
- The server asks Meilisearch for `limit + offset` hits and skips `offset` of them itself.
- There is no host filter and no snippet. The API's `GET /api/v1/search` has both.

## Configuration

The server reads the section `Meilisearch` (`MeilisearchOptions`):

| Setting | Default | Meaning |
|---|---|---|
| `Meilisearch:Host` | `http://localhost:7700` | Meilisearch URL |
| `Meilisearch:ApiKey` | `masterKey` | Meilisearch key |
| `Meilisearch:IndexName` | `pages` | Index to search |

The defaults in `MeilisearchOptions` and in `appsettings.json` are the same. They match `docker compose up` on the same machine:
compose publishes Meilisearch on `127.0.0.1:7700` with the key `MEILI_MASTER_KEY` (default `masterKey`). If you changed
`MEILI_MASTER_KEY` in `.env`, set `Meilisearch__ApiKey` to the same value. The host builder (`Host.CreateApplicationBuilder`)
reads environment variables such as `Meilisearch__ApiKey` and command-line arguments such as `--Meilisearch:ApiKey=...`.

Before its first search the server applies the index settings (`MeilisearchSettings.Ensure`), so the key must be allowed to
change the settings of the index. The master key is.

## Run

Start the crawler first, so Meilisearch runs and has pages (see the [README](../../README.md#how-to-run)). Then, from the
repository root:

```bash
dotnet run --project src/SamoBot.Mcp
```

The server then waits for MCP messages on stdin. An MCP client starts it this way; you do not use it from a terminal.

## Register with Claude Code

```bash
claude mcp add --transport stdio samobot -- dotnet run --project /absolute/path/to/web-crawler/src/SamoBot.Mcp
```

With a changed Meilisearch key:

```bash
claude mcp add --transport stdio --env Meilisearch__ApiKey=<MEILI_MASTER_KEY> samobot \
  -- dotnet run --project /absolute/path/to/web-crawler/src/SamoBot.Mcp
```

## Register with Claude Desktop

Add the server to `claude_desktop_config.json` (on macOS in `~/Library/Application Support/Claude/`, on Windows in
`%APPDATA%\Claude\`) and restart Claude Desktop:

```json
{
  "mcpServers": {
    "samobot": {
      "command": "dotnet",
      "args": ["run", "--project", "/absolute/path/to/web-crawler/src/SamoBot.Mcp"],
      "env": { "Meilisearch__ApiKey": "masterKey" }
    }
  }
}
```

## Example

Question: "Which crawled pages explain rate limiting? Give me the five best."

The assistant calls the tool like this:

```json
{
  "name": "search_pages",
  "arguments": { "query": "rate limiting", "limit": 5 }
}
```

The result is a list of up to five matches, each with the page title and its canonical URL.

## Packaging

The project file can pack the server as a .NET tool with the package ID `SamoBot.Mcp` (`PackAsTool`, self-contained
single-file builds for six runtimes), and `.mcp/server.json` describes it for an MCP registry. The server is not published
as a package yet. Run it from source as above.
