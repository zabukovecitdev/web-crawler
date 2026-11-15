using SamoBot.Core.Domain;
using SamoBot.Core.Parsing;

namespace SamoBot.Core.Directives;

/// <summary>
/// Chooses the URLs a parsed page adds to the frontier. Links are queued one level deeper when the policy allows following
/// and every <see cref="ILinkFollowRule"/> allows the link. Redirect targets (<see cref="IRedirectSource"/>) are queued at the
/// depth of the page, like an HTTP redirect, whatever the policy says about links. The job scope applies to both.
/// </summary>
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

        // A URL that is both a redirect target and a link keeps the redirect entry, at the depth of the page.
        return JobScope.FilterAtDepth(page.Fetch, redirects, page.Fetch.Depth)
            .Concat(JobScope.Filter(page.Fetch, links))
            .DistinctBy(url => url.NormalizedUrl)
            .ToList();
    }
}
