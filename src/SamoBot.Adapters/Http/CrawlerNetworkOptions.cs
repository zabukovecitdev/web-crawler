namespace SamoBot.Adapters.Http;

public sealed class CrawlerNetworkOptions
{
    public const string SectionName = "Crawler";

    public bool AllowPrivateAddresses { get; set; }

    public bool UseProxy { get; set; }

    public int DnsCacheMinutes { get; set; } = 60;

    public int MaxAdaptiveDelaySeconds { get; set; } = 60;

    public int InFlightMarginSeconds { get; set; } = 5;

    public int ConnectTimeoutSeconds { get; set; } = 5;

    public int FirstByteTimeoutSeconds { get; set; } = 10;

    public int RequestTimeoutSeconds { get; set; } = 20;

    public int RobotsTimeoutSeconds { get; set; } = 10;

    public int SitemapTimeoutSeconds { get; set; } = 30;
}
