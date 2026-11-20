namespace SamoBot.Core.Sitemaps;

public static class SitemapLimits
{
    public const int MaxIndexDepth = 2;
    public const int MaxFilesPerHost = 50;
    public const int MaxUrlsPerFile = 50_000;
    public const int MaxUrlsPerHost = 500_000;
    public const int MaxCompressedBytes = 10 * 1024 * 1024;
    public const int MaxDecompressedBytes = 50 * 1024 * 1024;
    public const int MaxLocBytes = 2048;
    public static readonly TimeSpan MaxFetchWait = TimeSpan.FromSeconds(60);
}
