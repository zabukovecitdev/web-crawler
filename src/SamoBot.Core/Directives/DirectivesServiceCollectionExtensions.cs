using Microsoft.Extensions.DependencyInjection;
using SamoBot.Core.Directives.Canonical;
using SamoBot.Core.Directives.Links;
using SamoBot.Core.Directives.Rules;
using SamoBot.Core.Directives.Sources;

namespace SamoBot.Core.Directives;

/// <summary>
/// The composition of the crawl directives. This is the one place to change what the crawler obeys: add, remove or reorder a
/// source or rule here. Each class is registered under every directive interface it implements.
/// </summary>
public static class DirectivesServiceCollectionExtensions
{
    private static readonly Type[] DirectiveInterfaces =
    [
        typeof(IRobotsDirectiveSource), typeof(IPagePolicyRule), typeof(ILinkFollowRule),
        typeof(IRedirectSource), typeof(ICanonicalSource), typeof(IResponseHeaderConsumer)
    ];

    public static IServiceCollection AddCrawlDirectives(this IServiceCollection services) => services
        // Where directives come from.
        .AddDirective(new MetaRobotsTagSource())
        .AddDirective(new XRobotsTagSource())
        // Where a page names its canonical URL; the first one found wins.
        .AddDirective(new HtmlCanonicalSource())
        .AddDirective(new LinkHeaderCanonicalSource())
        // What the directives and signals mean for the page.
        .AddDirective(new NoFollowRule())
        .AddDirective(new NoIndexRule())
        .AddDirective(new NoSnippetRule())
        .AddDirective(new MaxSnippetRule())
        .AddDirective(new NoArchiveRule())
        .AddDirective(new UnavailableAfterRule())
        .AddDirective(new NonCanonicalPageRule())
        // Which single links are not followed.
        .AddDirective(new RelLinkRule("nofollow", "sponsored", "ugc"))
        // URLs a page sends the crawler to instead of itself.
        .AddDirective(new MetaRefreshSource())
        .AddDirective(new CanonicalTargetSource())
        // The services that apply them.
        .AddSingleton<ResponseHeaderSelection>()
        .AddSingleton<PageContextFactory>()
        .AddSingleton<PagePolicyEvaluator>()
        .AddSingleton<LinkSelector>();

    /// <summary>Registers one directive object under each directive interface it implements.</summary>
    public static IServiceCollection AddDirective(this IServiceCollection services, object directive)
    {
        var implemented = DirectiveInterfaces.Where(type => type.IsInstanceOfType(directive)).ToList();
        if (implemented.Count == 0)
        {
            throw new ArgumentException($"{directive.GetType().Name} implements no directive interface.", nameof(directive));
        }

        foreach (var type in implemented)
        {
            services.AddSingleton(type, directive);
        }

        return services;
    }
}
