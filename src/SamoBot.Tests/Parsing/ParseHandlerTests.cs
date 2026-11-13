using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Core.Parsing;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Parsing;

public class ParseHandlerTests
{
    private static readonly FetchToParse Fetch = new()
    {
        FetchId = 1,
        UrlId = 2,
        CrawlJobId = 7,
        Url = "https://a.example/x",
        Depth = 0,
        ObjectName = "page-1",
        AllowedHosts = ["a.example"]
    };

    private static ParsedPage Page(bool noFollow = false) => new()
    {
        Title = "T",
        Links = [new ParsedLink { Url = "/about" }, new ParsedLink { Url = "https://other.example/" }],
        RobotsDirectives = new RobotsDirectives { NoFollow = noFollow }
    };

    private static (FakeParseBacklog Queue, ParsePageHandler Handler) Build(ParsedPage page)
    {
        var store = new FakeContentStore();
        store.Put("page-1", "<html></html>");
        var queue = new FakeParseBacklog(Fetch);
        return (queue, new ParsePageHandler(store, new FakeHtmlParser(page), queue));
    }

    [Fact]
    public async Task HandleResolvesAndFiltersLinksThenCompletes()
    {
        var (queue, handler) = Build(Page());

        await handler.Handle(Fetch, default);

        var completed = queue.Completed.Should().ContainSingle().Subject;
        completed.Page.Title.Should().Be("T");
        completed.Links.Should().Equal(new NewUrl(7, "https://a.example/about", "https://a.example/about", "a.example", 1));
    }

    [Fact]
    public async Task HandleWithNoFollowDirectivePassesEmptyLinkList()
    {
        var (queue, handler) = Build(Page(noFollow: true));

        await handler.Handle(Fetch, default);

        queue.Completed.Single().Links.Should().BeEmpty();
    }

    [Fact]
    public async Task HandleBatchClaimsTenFetchesWithFiveMinuteLease()
    {
        var (queue, pageHandler) = Build(Page());

        var count = await new ParseBatchHandler(queue, pageHandler).HandleBatch(default);

        count.Should().Be(1);
        queue.LastClaim.Should().Be((10, TimeSpan.FromMinutes(5)));
        queue.Completed.Should().HaveCount(1);
    }
}
