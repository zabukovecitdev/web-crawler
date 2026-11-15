using Microsoft.Extensions.DependencyInjection;
using SamoBot.Core.Directives;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Fakes;

/// <summary>The crawl directives exactly as production composes them (<see cref="DirectivesServiceCollectionExtensions.AddCrawlDirectives"/>).</summary>
public static class TestDirectives
{
    private static readonly ServiceProvider Provider = new ServiceCollection().AddCrawlDirectives().BuildServiceProvider();

    public static PageContextFactory Contexts => Provider.GetRequiredService<PageContextFactory>();
    public static PagePolicyEvaluator Policies => Provider.GetRequiredService<PagePolicyEvaluator>();
    public static LinkSelector Links => Provider.GetRequiredService<LinkSelector>();
    public static ResponseHeaderSelection Headers => Provider.GetRequiredService<ResponseHeaderSelection>();

    /// <summary>The context of a page at <paramref name="url"/>, with optional kept response headers.</summary>
    public static PageContext Context(ParsedPage page, string url = "https://a.example/x", ResponseHeaders? headers = null) =>
        Contexts.Create(Fetch(url, headers), page);

    public static PagePolicy Evaluate(ParsedPage page, string url = "https://a.example/x", ResponseHeaders? headers = null) =>
        Policies.Evaluate(Context(page, url, headers));

    public static FetchToParse Fetch(string url = "https://a.example/x", ResponseHeaders? headers = null, int depth = 0) => new()
    {
        FetchId = 1,
        UrlId = 2,
        CrawlJobId = 7,
        Url = url,
        Depth = depth,
        ObjectName = "page-1",
        AllowedHosts = [new Uri(url).Authority],
        ResponseHeaders = headers ?? ResponseHeaders.None
    };

    public static ResponseHeaders Header(string name, params string[] values) =>
        new([new KeyValuePair<string, IReadOnlyList<string>>(name, values)]);

    public static ParsedPage Meta(string name, string content, params ParsedLink[] links) =>
        new() { MetaTags = [new MetaTag(name, content)], Links = links };
}
