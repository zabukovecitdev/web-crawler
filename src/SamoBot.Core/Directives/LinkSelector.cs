using SamoBot.Core.Domain;
using SamoBot.Core.Parsing;

namespace SamoBot.Core.Directives;

public sealed class LinkSelector(IEnumerable<ILinkFollowRule> linkRules, IEnumerable<IRedirectSource> redirectSources)
{
    private readonly IReadOnlyList<ILinkFollowRule> _linkRules = linkRules.ToList();
    private readonly IReadOnlyList<IRedirectSource> _redirectSources = redirectSources.ToList();

    public bool IsFollowable(ParsedLink link, PageContext page) => _linkRules.All(rule => rule.Allows(link, page));

    public IReadOnlyList<NewUrl> UrlsToQueue(PageContext page, PagePolicy policy)
    {
        var redirects = _redirectSources.SelectMany(source => source.Targets(page)).Select(LinkResolver.Normalize).OfType<string>().Distinct().ToList();
        var links = policy.FollowLinks
            ? LinkResolver.Resolve(page.BaseUrl, page.Page.Links.Where(link => IsFollowable(link, page)))
            : [];

        return JobScope.FilterAtDepth(page.Fetch, redirects, page.Fetch.Depth)
            .Concat(JobScope.Filter(page.Fetch, links))
            .DistinctBy(url => url.NormalizedUrl)
            .ToList();
    }
}
