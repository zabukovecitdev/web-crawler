using Microsoft.Extensions.Logging;
using Neo4j.Driver;
using SamoBot.Infrastructure.Graph.Abstractions;
using SamoBot.Infrastructure.Models;

namespace SamoBot.Infrastructure.Graph;

public class PageGraphRepository : IPageGraphRepository
{
    private readonly IDriver _driver;
    private readonly ILogger<PageGraphRepository> _logger;

    private const string UpsertQuery = """
        MERGE (source:Page {url: $sourceUrl})
        SET source.normalizedUrl    = $normalizedUrl,
            source.host             = $host,
            source.discoveredUrlId  = $discoveredUrlId,
            source.parsedDocumentId = $parsedDocumentId
        WITH source
        UNWIND $links AS link
          MERGE (target:Page {url: link.url})
          SET target.host = link.host
          MERGE (source)-[r:LINKS_TO {targetUrl: link.url}]->(target)
          SET r.anchorText  = link.anchorText,
              r.rel         = link.rel,
              r.isNoFollow  = link.isNoFollow,
              r.extractedAt = link.extractedAt
        """;

    public PageGraphRepository(IDriver driver, ILogger<PageGraphRepository> logger)
    {
        _driver = driver;
        _logger = logger;
    }

    public async Task UpsertPageLinksAsync(
        string sourceUrl,
        string normalizedSourceUrl,
        string sourceHost,
        int discoveredUrlId,
        int parsedDocumentId,
        IReadOnlyList<ParsedLink> links,
        CancellationToken cancellationToken = default)
    {
        var resolvedLinks = ResolveLinks(links, sourceUrl);

        await using var session = _driver.AsyncSession();
        await session.RunAsync(UpsertQuery, new
        {
            sourceUrl,
            normalizedUrl = normalizedSourceUrl,
            host = sourceHost,
            discoveredUrlId,
            parsedDocumentId,
            links = resolvedLinks
        });

        _logger.LogDebug("Upserted {LinkCount} links for {SourceUrl} in Neo4j", resolvedLinks.Count, sourceUrl);
    }

    private static List<object> ResolveLinks(IReadOnlyList<ParsedLink> links, string sourceUrl)
    {
        if (!Uri.TryCreate(sourceUrl, UriKind.Absolute, out var sourceUri))
            return [];

        var extractedAt = DateTime.UtcNow;
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var result = new List<object>();

        foreach (var link in links)
        {
            if (string.IsNullOrWhiteSpace(link.Url))
                continue;

            if (link.Url.Contains(':', StringComparison.OrdinalIgnoreCase) &&
                !link.Url.StartsWith("http://", StringComparison.OrdinalIgnoreCase) &&
                !link.Url.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
                continue;

            Uri? absoluteUri = null;
            try
            {
                if (!Uri.TryCreate(link.Url, UriKind.Absolute, out absoluteUri))
                    Uri.TryCreate(sourceUri, link.Url, out absoluteUri);
            }
            catch
            {
                continue;
            }

            if (absoluteUri is null)
                continue;

            if (absoluteUri.Scheme != Uri.UriSchemeHttp && absoluteUri.Scheme != Uri.UriSchemeHttps)
                continue;

            // Strip fragment
            var builder = new UriBuilder(absoluteUri) { Fragment = "" };
            var normalizedUrl = builder.Uri.AbsoluteUri;

            if (!seen.Add(normalizedUrl))
                continue;

            result.Add(new
            {
                url = normalizedUrl,
                host = absoluteUri.Host,
                anchorText = link.LinkText,
                rel = link.Rel,
                isNoFollow = link.IsNoFollow,
                extractedAt
            });
        }

        return result;
    }
}
