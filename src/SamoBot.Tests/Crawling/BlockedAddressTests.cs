using FluentAssertions;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Adapters.Browser;
using SamoBot.Adapters.Http;
using SamoBot.Core.Common;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Crawling;

public class BlockedAddressTests
{
    private readonly FakeTimeProvider _time = new(TestData.Now);
    private readonly FakeUrlFrontier _frontier = new();
    private readonly FakeEventPublisher _events = new();
    private readonly FakeContentStore _store = new();

    private sealed class BlockedRobotsSource : IRobotsSource
    {
        public Task<RobotsRules> Download(string host, CancellationToken ct) => throw new BlockedAddressException(host);
    }

    private CrawlUrlHandler Handler(IFetcher fetcher, IRobotsSource robotsSource)
    {
        var gate = new RobotsGate(new MemoryCache(Options.Create(new MemoryCacheOptions())), new FakeRobotsStore(), robotsSource, _time);
        return new CrawlUrlHandler(gate, new FetcherResolver([fetcher]), _store, _frontier, _events, _time);
    }

    private static FakeRobotsSource OpenRobots() =>
        new(new RobotsRules("a.example", "User-agent: *\nDisallow:\n", TestData.Now, TestData.Now.AddDays(1)));

    [Fact]
    public void ClassifyABlockedResponseReturnsBlockedWhateverTheStatusAndFailCount()
    {
        OutcomeClassifier.Classify(FetchResponse.BlockedAddress(), 4).Should().Be(new Blocked());
    }

    [Fact]
    public void BlockedDisablesTheUrlForeverWithoutRetryOrBackoff()
    {
        var update = new Blocked().ToUpdate(TestData.Url(failCount: 2), TestData.Now);

        update.Should().Be(new UrlUpdate(UrlStatus.Disabled, null, 2, "blocked-address", null));
    }

    [Fact]
    public void ContentStatusOfABlockedResponseIsSkipped()
    {
        ContentStatus.Of(FetchResponse.BlockedAddress(), null).Should().Be(ContentStatus.Skipped);
    }

    [Fact]
    public async Task HandleWhenTheFetcherIsBlockedDisablesRecordsStatusZeroAndPublishesBlocked()
    {
        var handler = Handler(new FakeFetcher(response: FetchResponse.BlockedAddress()), OpenRobots());

        await handler.Handle(TestData.Url(), default);

        var completed = _frontier.Completed.Should().ContainSingle().Subject;
        completed.Update.Should().Be(new UrlUpdate(UrlStatus.Disabled, null, 0, "blocked-address", null));
        completed.Fetch!.StatusCode.Should().Be(0);
        AssertPublishedBlocked();
    }

    [Fact]
    public async Task HandleWhenTheRobotsHostIsBlockedDoesNotFetchAndDisablesTheUrl()
    {
        var fetcher = new FakeFetcher();
        var handler = Handler(fetcher, new BlockedRobotsSource());

        await handler.Handle(TestData.Url(), default);

        fetcher.Fetched.Should().BeEmpty();
        _frontier.Completed.Should().ContainSingle().Which.Update.FailReason.Should().Be("blocked-address");
        AssertPublishedBlocked();
    }

    private void AssertPublishedBlocked()
    {
        var published = _events.Published.Single(e => e.EventType == CrawlEventTypes.FetchCompleted).Payload;
        var properties = published.GetType().GetProperties().ToDictionary(p => p.Name, p => p.GetValue(published));
        properties["outcome"].Should().Be("Blocked");
        properties["contentStatus"].Should().Be(ContentStatus.Skipped);
    }

    [Fact]
    public async Task RobotsGateRulesForADashboardLookupOfABlockedHostDisallowsEverything()
    {
        var gate = new RobotsGate(
            new MemoryCache(Options.Create(new MemoryCacheOptions())), new FakeRobotsStore(), new BlockedRobotsSource(), _time);

        var rules = await gate.RulesFor("evil.example", default);

        rules.IsAllowed("/", RobotsGate.UserAgent).Should().BeFalse();
    }

    [Fact]
    public async Task HttpFetcherTurnsABlockedConnectIntoABlockedResponse()
    {
        var handler = new StubHttpHandler(_ => throw new HttpRequestException("connect", new BlockedAddressException("evil.example")));
        var fetcher = new HttpFetcher(new HttpClient(handler), Options.Create(new ChromeRenderingOptions()), _time);

        var response = await fetcher.Fetch(TestData.Url(), default);

        response.IsBlocked.Should().BeTrue();
        response.StatusCode.Should().Be(0);
    }

    [Fact]
    public async Task HttpRobotsSourceRethrowsABlockedHostInsteadOfTryingHttp()
    {
        var handler = new StubHttpHandler(_ => throw new HttpRequestException("connect", new BlockedAddressException("evil.example")));
        var source = new HttpRobotsSource(new HttpClient(handler), _time);

        var act = () => source.Download("evil.example", default);

        await act.Should().ThrowAsync<BlockedAddressException>();
        handler.Requests.Should().ContainSingle();
    }
}
