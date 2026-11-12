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
    public void CrawlJob_HasStringStatusAndAJsonStringOfSeedUrls()
    {
        var job = CrawlJob.Create(new NewCrawlJob("u1", ["https://a.example/"], 2, 10, false, true), TestData.Now).Value;

        var json = Serialize(CrawlJobResponse.From(job));

        json.GetProperty("status").GetString().Should().Be("Pending");
        json.GetProperty("seedUrls").GetString().Should().Be("""["https://a.example/"]""");
        json.EnumerateObject().Select(p => p.Name).Should().BeEquivalentTo(
            "id", "ownerUserId", "status", "seedUrls", "maxDepth", "maxUrls", "useJsRendering", "respectRobots",
            "createdAt", "updatedAt", "startedAt", "completedAt");
    }

    [Fact]
    public void JobPage_HasAStringStatusAndTheFieldsTheDashboardReads()
    {
        var json = Serialize(new JobPage { Id = 1, Host = "a.example", Url = "https://a.example/", Status = UrlStatus.InFlight, Depth = 1 });

        json.GetProperty("status").GetString().Should().Be("InFlight");
        json.EnumerateObject().Select(p => p.Name).Should().Contain(
            ["id", "host", "url", "normalizedUrl", "status", "crawlJobId", "depth", "useJsRendering", "respectRobots"]);
    }

    [Fact]
    public void JobEvent_HasAStringPayload()
    {
        var json = Serialize(new JobEvent { Id = 3, CrawlJobId = 1, EventType = "JobStarted", Payload = "{}", CreatedAt = TestData.Now });

        json.EnumerateObject().Select(p => p.Name).Should().BeEquivalentTo("id", "crawlJobId", "eventType", "payload", "createdAt");
        json.GetProperty("payload").ValueKind.Should().Be(JsonValueKind.String);
    }
}
