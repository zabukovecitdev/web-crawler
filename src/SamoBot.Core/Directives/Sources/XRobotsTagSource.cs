namespace SamoBot.Core.Directives.Sources;

/// <summary>The X-Robots-Tag response header. Each value may be aimed at one crawler; values for other crawlers are ignored.</summary>
public sealed class XRobotsTagSource : IRobotsDirectiveSource, IResponseHeaderConsumer
{
    public const string HeaderName = "X-Robots-Tag";

    public IReadOnlyCollection<string> ResponseHeaders { get; } = [HeaderName];

    public IEnumerable<RobotsDirective> Read(PageContext page) =>
        page.Headers.Values(HeaderName).SelectMany(line => RobotsDirectiveParser.ParseScoped(line, CrawlerIdentity.Name, HeaderName));
}
