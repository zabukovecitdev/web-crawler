using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Domain;

public class CrawlProgressTests
{
    private static JobProgressData Data(string status = "Running") => new()
    {
        JobId = 3,
        Status = status,
        MaxUrls = 100,
        StartedAt = TestData.Now.AddMinutes(-10),
        Crawled = 20,
        Queued = 5,
        InFlight = 1,
        Failed = 2,
        CurrentUrl = "https://a.example/x",
        CurrentDepth = 2,
        CrawlDelayMs = 2000,
        Fetches = 30,
        AvgResponseMs = 123.456,
        MaxResponseMs = 900,
        Ok = 20,
        Redirect = 4,
        ClientError = 5,
        ServerError = 1
    };

    [Fact]
    public void MapsCountersCurrentUrlAndStatusClasses()
    {
        var progress = CrawlProgress.From(Data(), TestData.Now);

        progress.Should().BeEquivalentTo(new
        {
            JobId = 3, Status = "Running", Crawled = 20, Queued = 5, InFlight = 1, Failed = 2, MaxUrls = 100,
            CurrentUrl = "https://a.example/x", CurrentDepth = 2, CrawlDelayMs = 2000, MaxResponseMs = 900L,
            StatusCounts = new StatusClassCounts(20, 4, 5, 1)
        });
        progress.AvgResponseMs.Should().Be(123.5);
    }

    [Fact]
    public void DelaySourceIsRobotsOnlyWhenRobotsSetTheDelay()
    {
        CrawlProgress.From(Data(), TestData.Now).DelaySource.Should().Be("default");
        CrawlProgress.From(Data() with { DelayFromRobots = true }, TestData.Now).DelaySource.Should().Be("robots.txt");
    }

    [Fact]
    public void PagesPerMinuteIsFetchesOverElapsedTime()
    {
        CrawlProgress.From(Data(), TestData.Now).PagesPerMinute.Should().Be(3);
        CrawlProgress.From(Data() with { CompletedAt = TestData.Now.AddMinutes(-5) }, TestData.Now).PagesPerMinute.Should().Be(6);
    }

    [Fact]
    public void PagesPerMinuteHasAOneMinuteFloorAndIsNullBeforeTheStart()
    {
        CrawlProgress.From(Data() with { StartedAt = TestData.Now.AddSeconds(-6), Fetches = 3 }, TestData.Now).PagesPerMinute.Should().Be(3);
        CrawlProgress.From(Data() with { StartedAt = null }, TestData.Now).PagesPerMinute.Should().BeNull();
    }

    [Fact]
    public void NextRequestIsSetOnlyForARunningJobWithAFutureHostTime()
    {
        var later = TestData.Now.AddSeconds(2);

        CrawlProgress.From(Data() with { NextAllowedAt = later }, TestData.Now).NextRequestAt.Should().Be(later);
        CrawlProgress.From(Data() with { NextAllowedAt = TestData.Now.AddSeconds(-1) }, TestData.Now).NextRequestAt.Should().BeNull();
        CrawlProgress.From(Data("Paused") with { NextAllowedAt = later }, TestData.Now).NextRequestAt.Should().BeNull();
        CrawlProgress.From(Data(), TestData.Now).NextRequestAt.Should().BeNull();
    }

    [Fact]
    public void KeepsMissingResponseTimesNull()
    {
        var progress = CrawlProgress.From(Data() with { AvgResponseMs = null, MaxResponseMs = null }, TestData.Now);

        progress.AvgResponseMs.Should().BeNull();
        progress.MaxResponseMs.Should().BeNull();
    }
}
