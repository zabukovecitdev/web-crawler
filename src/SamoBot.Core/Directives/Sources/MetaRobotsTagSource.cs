namespace SamoBot.Core.Directives.Sources;

public sealed class MetaRobotsTagSource : IRobotsDirectiveSource
{
    private static readonly string[] Names = ["robots", CrawlerIdentity.Name];

    public IEnumerable<RobotsDirective> Read(PageContext page) =>
        Names.SelectMany(page.MetaNamed)
            .SelectMany(tag => RobotsDirectiveParser.ParseList(tag.Content, $"meta {tag.Name.ToLowerInvariant()}"));
}
