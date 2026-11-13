using FluentAssertions;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Crawling;

public class CrawlBatchHandlerTests
{
    [Fact]
    public async Task HandleBatchClaimsTwentyUrlsWithFiveMinuteLeaseAndCrawlsEach()
    {
        var time = new FakeTimeProvider(TestData.Now);
        var frontier = new FakeUrlFrontier(TestData.Url("https://a.example/1"), TestData.Url("https://b.example/2"));
        var rules = RobotsRules.AllowAll("x", TestData.Now, TimeSpan.FromDays(1));
        var gate = new RobotsGate(
            new MemoryCache(Options.Create(new MemoryCacheOptions())),
            new FakeRobotsStore(),
            new FakeRobotsSource(rules),
            time);
        var urlHandler = new CrawlUrlHandler(
            gate,
            new FetcherResolver([new FakeFetcher()]),
            new FakeContentStore(),
            frontier,
            new FakeEventPublisher(),
            time);

        var count = await new CrawlBatchHandler(frontier, urlHandler).HandleBatch(default);

        count.Should().Be(2);
        frontier.LastClaim.Should().Be((20, TimeSpan.FromMinutes(5)));
        frontier.Completed.Should().HaveCount(2);
    }
}
