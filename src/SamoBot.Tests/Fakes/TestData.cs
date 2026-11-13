using SamoBot.Core.Domain;

namespace SamoBot.Tests.Fakes;

public static class TestData
{
    public static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    public static ClaimedUrl Url(
        string url = "https://a.example/x",
        int failCount = 0,
        bool respectRobots = true,
        bool useJsRendering = false)
    {
        var uri = new Uri(url);
        return new ClaimedUrl
        {
            Id = 1,
            CrawlJobId = 7,
            Host = uri.Host,
            Url = url,
            NormalizedUrl = url,
            Depth = 0,
            FailCount = failCount,
            UseJsRendering = useJsRendering,
            RespectRobots = respectRobots
        };
    }

    public static FetchResponse Response(int status, string? location = null, string? contentType = "text/html") => new()
    {
        StatusCode = status,
        ContentType = contentType,
        Content = status is >= 200 and <= 299 ? "<html></html>"u8.ToArray() : null,
        RedirectLocation = location
    };
}
