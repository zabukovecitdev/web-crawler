using FluentAssertions;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
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
    public async Task Handle_WhenRobotsBlock_DisablesUrlAndDoesNotFetch()
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
    public async Task Handle_HtmlSuccess_StoresContentCompletesAndPublishes()
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
    public async Task Handle_ServerError_SchedulesRetryWithoutStoringContent()
    {
        var handler = Handler(new FakeFetcher(response: TestData.Response(500)));

        await handler.Handle(TestData.Url(failCount: 1), default);

        _store.SavedUrls.Should().BeEmpty();
        var update = _frontier.Completed.Single().Update;
        update.Should().Be(new UrlUpdate(UrlStatus.Idle, TestData.Now.AddMinutes(2), 2, "http-500", null));
    }

    [Fact]
    public async Task Handle_NonHtmlContent_IsNotStored()
    {
        var pdf = new FetchResponse { StatusCode = 200, ContentType = "application/pdf", Content = "%PDF-1.4 binary"u8.ToArray() };
        var handler = Handler(new FakeFetcher(response: pdf));

        await handler.Handle(TestData.Url(), default);

        _store.SavedUrls.Should().BeEmpty();
        _frontier.Completed.Single().Fetch!.ObjectName.Should().BeNull();
    }

    [Fact]
    public async Task Handle_HtmlErrorPage_IsNotStored()
    {
        var response = new FetchResponse { StatusCode = 404, ContentType = "text/html", Content = "<html>missing</html>"u8.ToArray() };
        var handler = Handler(new FakeFetcher(response: response));

        await handler.Handle(TestData.Url(), default);

        _store.SavedUrls.Should().BeEmpty();
    }
}
