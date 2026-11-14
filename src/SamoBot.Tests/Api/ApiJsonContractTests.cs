using System.Text.Json;
using FluentAssertions;
using SamoBot.Api.Contracts;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Api;

/// <summary>The dashboard reads these shapes. They must not change.</summary>
public class ApiJsonContractTests
{
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private static JsonElement Serialize<T>(T value) => JsonSerializer.SerializeToElement(value, Web);

    [Fact]
    public void CrawlJobMatchesTheDtoTypeWithARealSeedArray()
    {
        var job = CrawlJob.Create(new NewCrawlJob("u1", ["https://a.example/", "https://b.example/x"], 2, 10, false, true), TestData.Now).Value;

        var json = Serialize(CrawlJobDto.From(job));

        json.GetProperty("status").GetString().Should().Be("Pending");
        json.GetProperty("host").GetString().Should().Be("a.example");
        json.GetProperty("seedUrls").EnumerateArray().Select(e => e.GetString()).Should().Equal("https://a.example/", "https://b.example/x");
        json.EnumerateObject().Select(p => p.Name).Should().BeEquivalentTo(
            "id", "status", "host", "seedUrls", "maxDepth", "maxUrls", "useJsRendering", "respectRobots",
            "createdAt", "startedAt", "completedAt");
    }

    [Fact]
    public void JobPageHasAStringStatusAndTheFieldsTheDashboardReads()
    {
        var json = Serialize(new JobPage { Id = 1, Host = "a.example", Url = "https://a.example/", Status = UrlStatus.InFlight, Depth = 1 });

        json.GetProperty("status").GetString().Should().Be("InFlight");
        json.EnumerateObject().Select(p => p.Name).Should().Contain(
            ["id", "host", "url", "normalizedUrl", "status", "crawlJobId", "depth", "useJsRendering", "respectRobots"]);
    }

    [Fact]
    public void StoredEventIsFlatAndCarriesTheCursorId()
    {
        var json = Serialize(CrawlEventEnvelope.Build(1, "FetchStarted", TestData.Now, """{"url":"https://a.example/","depth":2}""", 3));

        json.EnumerateObject().Select(p => p.Name).Should().BeEquivalentTo("id", "crawlJobId", "eventType", "at", "url", "depth");
        json.GetProperty("id").GetInt64().Should().Be(3);
        json.GetProperty("depth").GetInt32().Should().Be(2);
    }

    [Fact]
    public void ProgressMatchesTheTypeScriptShape()
    {
        var json = Serialize(CrawlProgress.From(new JobProgressData { JobId = 1, Status = "Running" }, TestData.Now));

        json.EnumerateObject().Select(p => p.Name).Should().BeEquivalentTo(
            "jobId", "status", "crawled", "queued", "inFlight", "failed", "maxUrls", "currentUrl", "currentDepth", "crawlDelayMs",
            "delaySource", "nextRequestAt", "avgResponseMs", "maxResponseMs", "pagesPerMinute", "startedAt", "statusCounts");
        json.GetProperty("statusCounts").EnumerateObject().Select(p => p.Name).Should().BeEquivalentTo(
            "ok", "redirect", "clientError", "serverError");
    }
}
