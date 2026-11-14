using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Sitemaps;

/// <summary>Builds the sitemap view of a host. A URL that is not crawled is Blocked when robots.txt disallows it for our user agent.</summary>
public sealed class SitemapOverviewService(ISitemapReader reader, RobotsGate robots)
{
    public async Task<SitemapOverview> Read(string host, int limit, int offset, CancellationToken ct)
    {
        var snapshot = await reader.Read(host, limit, offset, ct);
        if (snapshot.Files.Count == 0)
        {
            return new SitemapOverview(host, snapshot.Files, snapshot.Counts, new PagedResult<SitemapUrlItem>(0, []));
        }

        var rules = await robots.RulesFor(host, ct);
        var items = snapshot.Urls.Items.Select(row => ToItem(row, rules)).ToList();
        return new SitemapOverview(host, snapshot.Files, snapshot.Counts, new PagedResult<SitemapUrlItem>(snapshot.Urls.Total, items));
    }

    public static SitemapUrlItem ToItem(SitemapUrlRow row, RobotsRules rules) =>
        new(row.Loc, row.LastModified, StateOf(row, rules), row.StatusCode, row.PageId);

    private static SitemapCrawlState StateOf(SitemapUrlRow row, RobotsRules rules)
    {
        if (row.PageId is not null)
        {
            return SitemapCrawlState.Crawled;
        }

        return IsDisallowed(rules, row.Loc) ? SitemapCrawlState.Blocked : SitemapCrawlState.NotCrawled;
    }

    private static bool IsDisallowed(RobotsRules rules, string loc) =>
        Uri.TryCreate(loc, UriKind.Absolute, out var uri) && !rules.IsAllowed(uri.PathAndQuery, RobotsGate.UserAgent);
}
