using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Parsing;

/// <summary>Parses one stored fetch and hands the page and its in-scope links to the parse queue.</summary>
public sealed class ParsePageHandler(IContentStore contentStore, IHtmlParser parser, IParseBacklog queue)
{
    public async Task Handle(FetchToParse fetch, CancellationToken ct)
    {
        var page = await Parse(fetch, ct);
        await queue.Complete(fetch, page, LinksToFollow(fetch, page), ct);
    }

    private async Task<ParsedPage> Parse(FetchToParse fetch, CancellationToken ct)
    {
        await using var html = await contentStore.Load(fetch.ObjectName, ct);
        return parser.Parse(html);
    }

    private static IReadOnlyList<NewUrl> LinksToFollow(FetchToParse fetch, ParsedPage page)
    {
        if (page.RobotsDirectives.ForbidsFollow)
        {
            return [];
        }

        return JobScope.Filter(fetch, LinkResolver.Resolve(new Uri(fetch.Url), page.Links));
    }
}
