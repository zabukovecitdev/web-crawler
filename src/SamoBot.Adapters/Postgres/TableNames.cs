namespace SamoBot.Adapters.Postgres;

/// <summary>The table names for SQL strings. SQL in this project never hard-codes a table name.</summary>
internal static class TableNames
{
    public const string DiscoveredUrls = "DiscoveredUrls";
    public const string UrlFetches = "UrlFetches";
    public const string ParsedDocuments = "ParsedDocuments";
    public const string RobotsTxt = "RobotsTxt";
    public const string CrawlJobs = "CrawlJobs";
    public const string CrawlJobEvents = "CrawlJobEvents";
    public const string Hosts = "Hosts";
    public const string SitemapFiles = "SitemapFiles";
    public const string SitemapUrls = "SitemapUrls";
    public const string SitemapRequests = "SitemapRequests";
}
