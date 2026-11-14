using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Parsing;

/// <summary>
/// Parses one stored fetch and hands the page and its in-scope links to the parse queue.
/// A page-level nofollow (robots meta tag or X-Robots-Tag header) drops every link; a link-level rel="nofollow" drops that link.
/// </summary>
public sealed class ParsePageHandler(IContentStore contentStore, IHtmlParser parser, IParseBacklog queue)
{
    public async Task Handle(FetchToParse fetch, CancellationToken ct)
    {
        var page = await Parse(fetch, ct);
        await queue.Complete(fetch, page, LinksToFollow(fetch, page), PageLinkPreparer.Prepare(new Uri(fetch.Url), page.Links), ct);
    }

    private async Task<ParsedPage> Parse(FetchToParse fetch, CancellationToken ct)
    {
        await using var html = await contentStore.Load(fetch.ObjectName, ct);
        return parser.Parse(html);
    }

    private static IReadOnlyList<NewUrl> LinksToFollow(FetchToParse fetch, ParsedPage page)
    {
        if (page.RobotsDirectives.ForbidsFollow || XRobotsTag.ForbidsFollow(fetch.XRobotsTag, RobotsGate.UserAgent))
        {
            return [];
        }

        // rel="nofollow" is always respected: such links are stored, never queued.
        var followable = page.Links.Where(link => !link.IsNoFollow);
        return JobScope.Filter(fetch, LinkResolver.Resolve(new Uri(fetch.Url), followable));
    }
}
