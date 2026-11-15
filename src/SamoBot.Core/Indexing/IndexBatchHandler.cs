using SamoBot.Core.Common;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Indexing;

/// <summary>
/// Sends parsed pages to the search index. Only the newest page of each URL counts. A page whose policy allows indexing is
/// added or updated; any other page (noindex, a copy of a canonical page, past its unavailable_after date) is removed, so
/// a site that adds noindex later disappears from search on the next crawl.
/// </summary>
public sealed class IndexBatchHandler(IIndexBacklog queue, ISearchIndex searchIndex, TimeProvider timeProvider) : IWorkHandler
{
    private const int BatchSize = 50;

    public async Task<int> HandleBatch(CancellationToken ct)
    {
        var pages = await queue.Claim(BatchSize, ct);
        if (pages.Count == 0)
        {
            return 0;
        }

        var now = timeProvider.GetUtcNow();
        var latest = LatestPerUrl(pages);
        await searchIndex.AddOrUpdate(latest.Where(page => page.Policy.AllowsIndexingAt(now)).ToList(), ct);
        await searchIndex.Remove(latest.Where(page => !page.Policy.AllowsIndexingAt(now)).Select(page => page.DiscoveredUrlId).ToList(), ct);
        await queue.MarkIndexed(pages.Select(page => page.PageId).ToList(), ct);
        return pages.Count;
    }

    /// <summary>A recrawl parses the same URL again; the newest parsed page of a URL decides.</summary>
    public static IReadOnlyList<PageToIndex> LatestPerUrl(IReadOnlyList<PageToIndex> pages) =>
        pages.GroupBy(page => page.DiscoveredUrlId).Select(group => group.MaxBy(page => page.PageId)!).ToList();
}
