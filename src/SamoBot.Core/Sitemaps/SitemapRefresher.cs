using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Sitemaps;

/// <summary>
/// Walks the sitemaps of one host: the seeds first, then the children of the indexes (at most 2 levels, 50 files).
/// Each fetch waits for the host delay and respects robots.txt. Only a walk that found a file replaces the stored data.
/// </summary>
public sealed class SitemapRefresher(
    RobotsGate robots,
    ISitemapSource source,
    ISitemapStore store,
    TimeProvider timeProvider)
{
    public async Task<int> Refresh(string host, CancellationToken ct)
    {
        var rules = await robots.RulesFor(host, ct);
        var walk = new Walk(host, rules, SitemapDiscovery.Seeds(rules, host));
        while (walk.Pending.TryDequeue(out var item) && walk.Files.Count < SitemapLimits.MaxFilesPerHost)
        {
            await Visit(walk, item, ct);
        }

        await store.Complete(host, walk.Files, ct);
        return walk.Files.Count;
    }

    private async Task Visit(Walk walk, Step item, CancellationToken ct)
    {
        if (!walk.Visited.Add(item.Url) || !IsAllowed(walk.Rules, item.Url))
        {
            return;
        }

        var wait = await store.ReserveFetchSlot(walk.Host, ct);
        if (wait > TimeSpan.Zero)
        {
            await Task.Delay(wait < SitemapLimits.MaxFetchWait ? wait : SitemapLimits.MaxFetchWait, timeProvider, ct);
        }

        var download = await source.Download(item.Url, ct);
        var parsed = download.IsSuccess ? SitemapParser.Parse(download.Content!, walk.Host) : null;
        if (parsed is not null)
        {
            Accept(walk, item, parsed);
        }
    }

    private void Accept(Walk walk, Step item, SitemapParseResult parsed)
    {
        var urls = parsed.Kind == SitemapKind.UrlSet ? walk.TakeNewUrls(parsed.Entries) : [];
        var file = new SitemapFile(walk.Host, item.Url, item.Parent, parsed.Kind, urls.Count, timeProvider.GetUtcNow());
        walk.Files.Add(new SitemapFileContent(file, urls));

        if (parsed.Kind == SitemapKind.Index && item.Depth < SitemapLimits.MaxIndexDepth)
        {
            foreach (var child in parsed.Entries)
            {
                walk.Pending.Enqueue(new Step(child.Loc, item.Url, item.Depth + 1));
            }
        }
    }

    private static bool IsAllowed(RobotsRules rules, string url) =>
        Uri.TryCreate(url, UriKind.Absolute, out var uri) && rules.IsAllowed(uri.PathAndQuery, RobotsGate.UserAgent);

    private sealed record Step(string Url, string? Parent, int Depth);

    private sealed class Walk
    {
        private readonly HashSet<string> _seen = new(StringComparer.Ordinal);

        public Walk(string host, RobotsRules rules, IEnumerable<string> seeds)
        {
            Host = host;
            Rules = rules;
            foreach (var seed in seeds)
            {
                Pending.Enqueue(new Step(seed, null, 0));
            }
        }

        public string Host { get; }
        public RobotsRules Rules { get; }
        public Queue<Step> Pending { get; } = new();
        public HashSet<string> Visited { get; } = new(StringComparer.Ordinal);
        public List<SitemapFileContent> Files { get; } = [];

        /// <summary>Keeps the URLs that no earlier file had, up to the limit of the host.</summary>
        public List<SitemapEntry> TakeNewUrls(IEnumerable<SitemapEntry> entries)
        {
            var fresh = new List<SitemapEntry>();
            foreach (var entry in entries)
            {
                if (_seen.Count >= SitemapLimits.MaxUrlsPerHost)
                {
                    break;
                }

                if (_seen.Add(entry.Loc))
                {
                    fresh.Add(entry);
                }
            }

            return fresh;
        }
    }
}
