using SamoBot.Core.Browser;
using SamoBot.Core.Directives;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Parsing;

public sealed class ParsePageHandler(
    IContentStore contentStore,
    IPageParser parser,
    IParseBacklog queue,
    PageContextFactory contexts,
    PagePolicyEvaluator policies,
    LinkSelector links)
{
    public async Task Handle(FetchToParse fetch, CancellationToken ct)
    {
        var parsed = await Parse(fetch, ct);
        if (fetch.CanEscalate && JsDetector.IsSuspect(parsed.Shell))
        {
            await queue.Escalate(fetch, ct);
            return;
        }

        var context = contexts.Create(fetch, parsed);
        var policy = policies.Evaluate(context);
        var page = context.Page with { Canonical = context.Canonical is { } canonical ? LinkResolver.Normalize(canonical) ?? string.Empty : string.Empty };
        var pageLinks = PageLinkPreparer.Prepare(context, link => !links.IsFollowable(link, context));
        await queue.Complete(fetch, new ParseResult(page, policy, links.UrlsToQueue(context, policy), pageLinks), ct);
    }

    private async Task<ParsedPage> Parse(FetchToParse fetch, CancellationToken ct)
    {
        await using var content = await contentStore.Load(fetch.ObjectName, ct);
        return parser.Parse(content, fetch.ContentType);
    }
}
