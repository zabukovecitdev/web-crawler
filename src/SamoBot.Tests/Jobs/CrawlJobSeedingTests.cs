using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Core.Browser;
using SamoBot.Core.Domain;
using SamoBot.Core.Jobs;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Jobs;

public class CrawlJobSeedingTests
{
    private readonly FakeCrawlJobRepository _jobs = new();
    private readonly FakeUrlFrontier _frontier = new();
    private readonly FakeSitemapSeeder _seeder = new(1893);
    private readonly FakeEventPublisher _events = new();
    private readonly CrawlJobService _service;

    public CrawlJobSeedingTests()
    {
        var time = new FakeTimeProvider(TestData.Now);
        _service = new CrawlJobService(_jobs, _frontier, _events, new BrowserGate(new BrowserAvailability(new FakeBrowserProbe(), time)), _seeder, time);
    }

    private static CrawlJob Job(bool fromSitemap) => new()
    {
        Id = 5,
        SeedUrls = ["https://a.example/guide/install"],
        AllowedHosts = ["a.example"],
        SeedFromSitemap = fromSitemap
    };

    [Fact]
    public async Task AJobThatSeedsFromTheSitemapAddsTheStartPageFirstAndThenTheSitemapUrls()
    {
        _jobs.Add(Job(fromSitemap: true));

        await _service.Start(5, default);

        _frontier.AddedSeeds.Should().ContainSingle().Which.Seeds.Should().ContainSingle().Which.Url.Should().Be("https://a.example/guide/install");
        _seeder.Seeded.Should().Equal((5, "a.example"));
        _events.Published.Should().ContainSingle(e => e.EventType == CrawlEventTypes.JobStarted)
            .Which.Payload.Should().BeEquivalentTo(new { sitemapSeeds = 1893 });
    }

    [Fact]
    public async Task AJobWithoutASitemapStartsFromTheStartPageOnly()
    {
        _jobs.Add(Job(fromSitemap: false));

        await _service.Start(5, default);

        _seeder.Seeded.Should().BeEmpty();
        _frontier.AddedSeeds.Should().ContainSingle();
    }

    [Fact]
    public async Task ResumingAJobDoesNotSeedTheSitemapAgain()
    {
        var job = Job(fromSitemap: true);
        _jobs.Add(job);
        await _service.Start(5, default);
        await _service.Pause(5, default);

        await _service.Start(5, default);

        _seeder.Seeded.Should().HaveCount(1);
    }
}
