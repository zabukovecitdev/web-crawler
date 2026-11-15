using SamoBot.Core.Directives;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Parsing;

/// <summary>
/// Parses one stored fetch and hands the result to the parse queue. What the site allows (robots meta tags, X-Robots-Tag,
/// rel values, canonical, meta refresh, &lt;base&gt;) is decided by the crawl directives, not here.
/// </summary>
public sealed class ParsePageHandler(
    IContentStore contentStore,
    IHtmlParser parser,
    IParseBacklog queue,
    PageContextFactory contexts,
    PagePolicyEvaluator policies,
    LinkSelector links)
{
    public async Task Handle(FetchToParse fetch, CancellationToken ct)
    {
        var context = contexts.Create(fetch, await Parse(fetch, ct));
        var policy = policies.Evaluate(context);
        var page = context.Page with { Canonical = context.Canonical is { } canonical ? LinkResolver.Normalize(canonical) ?? string.Empty : string.Empty };
        var pageLinks = PageLinkPreparer.Prepare(context, link => !links.IsFollowable(link, context));
        await queue.Complete(fetch, new ParseResult(page, policy, links.UrlsToQueue(context, policy), pageLinks), ct);
    }

    private async Task<ParsedPage> Parse(FetchToParse fetch, CancellationToken ct)
    {
        await using var html = await contentStore.Load(fetch.ObjectName, ct);
        return parser.Parse(html);
    }
}
