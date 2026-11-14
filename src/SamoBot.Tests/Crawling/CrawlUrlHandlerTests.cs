using System.Text.Json;
using FluentAssertions;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Core.Common;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Crawling;

public class CrawlUrlHandlerTests
{
    private readonly FakeTimeProvider _time = new(TestData.Now);
    private readonly FakeContentStore _store = new();
    private readonly FakeUrlFrontier _frontier = new();
    private readonly FakeEventPublisher _events = new();

    private CrawlUrlHandler Handler(FakeFetcher fetcher, string robotsContent = "User-agent: *\nDisallow:\n")
    {
        var rules = new RobotsRules("a.example", robotsContent, TestData.Now, TestData.Now.AddDays(1));
        var gate = new RobotsGate(
            new MemoryCache(Options.Create(new MemoryCacheOptions())),
            new FakeRobotsStore(),
            new FakeRobotsSource(rules),
            _time);
        return new CrawlUrlHandler(gate, new FetcherResolver([fetcher]), _store, _frontier, _events, _time);
    }

    [Fact]
    public async Task HandleWhenRobotsBlockDisablesUrlAndDoesNotFetch()
    {
        var fetcher = new FakeFetcher();
        var handler = Handler(fetcher, "User-agent: *\nDisallow: /\n");

        await handler.Handle(TestData.Url(), default);

        fetcher.Fetched.Should().BeEmpty();
        var completed = _frontier.Completed.Should().ContainSingle().Subject;
        completed.Update.Should().Be(new UrlUpdate(UrlStatus.Disabled, null, 0, "robots", null));
        completed.Fetch.Should().BeNull();
    }

    [Fact]
    public async Task HandleHtmlSuccessStoresContentCompletesAndPublishes()
    {
        var url = TestData.Url();
        var handler = Handler(new FakeFetcher(response: TestData.Response(200)));

        await handler.Handle(url, default);

        _store.SavedUrls.Should().Equal(url.Url);
        var completed = _frontier.Completed.Should().ContainSingle().Subject;
        completed.Update.Should().Be(new UrlUpdate(UrlStatus.Idle, TestData.Now.AddDays(1), 0, null, null));
        completed.Fetch!.ObjectName.Should().Be("object-1");
        completed.Fetch.StatusCode.Should().Be(200);
        _events.Published.Should().ContainSingle(e => e.JobId == 7 && e.EventType == CrawlEventTypes.FetchCompleted);
    }

    [Fact]
    public async Task HandleServerErrorSchedulesRetryWithoutStoringContent()
    {
        var handler = Handler(new FakeFetcher(response: TestData.Response(500)));

        await handler.Handle(TestData.Url(failCount: 1), default);

        _store.SavedUrls.Should().BeEmpty();
        var update = _frontier.Completed.Single().Update;
        update.Should().Be(new UrlUpdate(UrlStatus.Idle, TestData.Now.AddMinutes(2), 2, "http-500", null));
    }

    [Fact]
    public async Task HandleNonHtmlContentIsNotStored()
    {
        var pdf = new FetchResponse { StatusCode = 200, ContentType = "application/pdf", Content = "%PDF-1.4 binary"u8.ToArray() };
        var handler = Handler(new FakeFetcher(response: pdf));

        await handler.Handle(TestData.Url(), default);

        _store.SavedUrls.Should().BeEmpty();
        _frontier.Completed.Single().Fetch!.ObjectName.Should().BeNull();
    }

    [Fact]
    public async Task HandleHtmlErrorPageIsNotStored()
    {
        var response = new FetchResponse { StatusCode = 404, ContentType = "text/html", Content = "<html>missing</html>"u8.ToArray() };
        var handler = Handler(new FakeFetcher(response: response));

        await handler.Handle(TestData.Url(), default);

        _store.SavedUrls.Should().BeEmpty();
    }

    [Fact]
    public async Task HandlePublishesFetchStartedBeforeFetchCompletedWithAllFields()
    {
        var response = new FetchResponse
        {
            StatusCode = 200, ContentType = "text/html", Content = "<html></html>"u8.ToArray(), ResponseTimeMs = 87
        };
        var url = TestData.Url() with { Depth = 3, CrawlDelayMs = 2500 };
        var handler = Handler(new FakeFetcher(response: response));

        await handler.Handle(url, default);

        _events.Published.Select(e => e.EventType).Should().Equal(CrawlEventTypes.FetchStarted, CrawlEventTypes.FetchCompleted);
        Json(_events.Published[0].Payload).Should().Be("""{"url":"https://a.example/x","depth":3}""");
        var completed = JsonSerializer.SerializeToElement(_events.Published[1].Payload, JsonDefaults.Options);
        completed.EnumerateObject().Select(p => p.Name).Should().BeEquivalentTo(
            "url", "depth", "statusCode", "responseTimeMs", "contentType", "contentLength", "contentStatus", "delayMs", "outcome");
        completed.GetProperty("statusCode").GetInt32().Should().Be(200);
        completed.GetProperty("responseTimeMs").GetInt64().Should().Be(87);
        completed.GetProperty("contentLength").GetInt32().Should().Be(13);
        completed.GetProperty("contentStatus").GetString().Should().Be("Stored");
        completed.GetProperty("delayMs").GetInt32().Should().Be(2500);
        completed.GetProperty("outcome").GetString().Should().Be("Succeeded");
    }

    [Fact]
    public async Task HandleReportsAFailedFetchWithNullContentFields()
    {
        var failure = new FetchResponse { StatusCode = 0, Error = "timeout" };
        var handler = Handler(new FakeFetcher(response: failure));

        await handler.Handle(TestData.Url(), default);

        var completed = JsonSerializer.SerializeToElement(_events.Published[1].Payload, JsonDefaults.Options);
        completed.GetProperty("contentStatus").GetString().Should().Be("Failed");
        completed.GetProperty("contentType").ValueKind.Should().Be(JsonValueKind.Null);
        completed.GetProperty("contentLength").ValueKind.Should().Be(JsonValueKind.Null);
        completed.GetProperty("outcome").GetString().Should().Be("RetryLater");
    }

    [Fact]
    public async Task HandlePublishesNoEventWhenRobotsBlockTheUrl()
    {
        var handler = Handler(new FakeFetcher(), "User-agent: *\nDisallow: /\n");

        await handler.Handle(TestData.Url(), default);

        _events.Published.Should().BeEmpty();
    }

    private static string Json(object payload) => JsonSerializer.Serialize(payload, JsonDefaults.Options);
}
