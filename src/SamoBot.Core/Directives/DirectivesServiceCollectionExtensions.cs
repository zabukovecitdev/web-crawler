using Microsoft.Extensions.DependencyInjection;
using SamoBot.Core.Directives.Canonical;
using SamoBot.Core.Directives.Links;
using SamoBot.Core.Directives.Rules;
using SamoBot.Core.Directives.Sources;

namespace SamoBot.Core.Directives;

public static class DirectivesServiceCollectionExtensions
{
    private static readonly Type[] DirectiveInterfaces =
    [
        typeof(IRobotsDirectiveSource), typeof(IPagePolicyRule), typeof(ILinkFollowRule),
        typeof(IRedirectSource), typeof(ICanonicalSource), typeof(IResponseHeaderConsumer)
    ];

    public static IServiceCollection AddCrawlDirectives(this IServiceCollection services) => services
        .AddDirective(new MetaRobotsTagSource())
        .AddDirective(new XRobotsTagSource())
        .AddDirective(new HtmlCanonicalSource())
        .AddDirective(new LinkHeaderCanonicalSource())
        .AddDirective(new NoFollowRule())
        .AddDirective(new NoIndexRule())
        .AddDirective(new NoSnippetRule())
        .AddDirective(new MaxSnippetRule())
        .AddDirective(new NoArchiveRule())
        .AddDirective(new UnavailableAfterRule())
        .AddDirective(new NonCanonicalPageRule())
        .AddDirective(new RelLinkRule("nofollow", "sponsored", "ugc"))
        .AddDirective(new MetaRefreshSource())
        .AddDirective(new CanonicalTargetSource())
        .AddSingleton<ResponseHeaderSelection>()
        .AddSingleton<PageContextFactory>()
        .AddSingleton<PagePolicyEvaluator>()
        .AddSingleton<LinkSelector>();

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
