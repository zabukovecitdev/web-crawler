namespace SamoBot.Core.Directives.Sources;

/// <summary>
/// &lt;meta name="robots"&gt; (every crawler) and &lt;meta name="samobot"&gt; (this crawler), matched without regard to case.
/// </summary>
public sealed class MetaRobotsTagSource : IRobotsDirectiveSource
{
    private static readonly string[] Names = ["robots", CrawlerIdentity.Name];

    public IEnumerable<RobotsDirective> Read(PageContext page) =>
        Names.SelectMany(page.MetaNamed)
            .SelectMany(tag => RobotsDirectiveParser.ParseList(tag.Content, $"meta {tag.Name.ToLowerInvariant()}"));
}
