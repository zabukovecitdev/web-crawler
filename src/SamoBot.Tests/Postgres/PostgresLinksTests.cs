using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Adapters.Postgres;
using SamoBot.Core.Directives;
using SamoBot.Core.Domain;
using SamoBot.Core.Parsing;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresLinksTests(PostgresFixture postgres) : IAsyncLifetime
{
    private static readonly TimeSpan Lease = TimeSpan.FromMinutes(5);

    private readonly FakeTimeProvider _time = new(TestData.Now);

    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    private PostgresParseBacklog Queue() => new(postgres.DataSource, _time);

    private PostgresPageReader Pages => new(postgres.DataSource);

    private static PageLink Link(string url, string? text = null, bool internalLink = true, bool noFollow = false) =>
        new(url, new Uri(url).Authority, text, internalLink, noFollow);

    private async Task<int> Page(int job, string url)
    {
        var urlId = await PostgresTestData.InsertUrl(postgres.DataSource, job, url);
        var fetchId = await PostgresTestData.InsertFetch(postgres.DataSource, urlId, "obj-" + urlId);
        await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """UPDATE "DiscoveredUrls" SET "LastFetchId" = @fetchId WHERE "Id" = @urlId RETURNING "Id" """,
            new { fetchId, urlId });
        return urlId;
    }

    private async Task Parse(int urlId, IReadOnlyList<PageLink> links)
    {
        await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """UPDATE "UrlFetches" SET "ParsedAt" = NULL, "LeaseUntil" = NULL WHERE "DiscoveredUrlId" = @urlId RETURNING "Id" """,
            new { urlId });
        var fetch = (await Queue().Claim(100, Lease, default)).Single(f => f.UrlId == urlId);
        await Queue().Complete(fetch, new ParseResult(new ParsedPage { Title = "T" }, PagePolicy.Default, [], links), default);
    }

    private Task<IReadOnlyList<string>> StoredTargets(int urlId) =>
        PostgresTestData.Rows<string>(postgres.DataSource, """SELECT "ToUrl" FROM "Links" WHERE "FromUrlId" = @urlId ORDER BY "Id" """, new { urlId });

    [Fact]
    public async Task CompleteStoresTheLinksWithThePage()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var home = await Page(job, "https://a.example/");

        await Parse(home, [Link("https://a.example/x", "  X ", noFollow: true), Link("https://b.example/y", null, internalLink: false)]);

        var rows = await PostgresTestData.Rows<dynamic>(postgres.DataSource, """SELECT * FROM "Links" ORDER BY "Id" """);
        rows.Should().HaveCount(2);
        ((string)rows[0].ToUrl).Should().Be("https://a.example/x");
        ((string)rows[0].ToHost).Should().Be("a.example");
        ((string)rows[0].AnchorText).Should().Be("  X ");
        ((bool)rows[0].NoFollow).Should().BeTrue();
        ((bool)rows[1].IsInternal).Should().BeFalse();
        ((object?)rows[1].AnchorText).Should().BeNull();
        ((DateTimeOffset)rows[0].FirstSeenAt).Should().Be(TestData.Now);
    }

    [Fact]
    public async Task AReparseReplacesTheLinksAndKeepsTheEarliestFirstSeen()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var home = await Page(job, "https://a.example/");
        await Parse(home, [Link("https://a.example/keep", "old"), Link("https://a.example/gone")]);
        _time.Advance(TimeSpan.FromHours(1));

        await Parse(home, [Link("https://a.example/keep", "new"), Link("https://a.example/added")]);

        (await StoredTargets(home)).Should().Equal("https://a.example/keep", "https://a.example/added");
        var keep = await PostgresTestData.Row(postgres.DataSource, """SELECT * FROM "Links" WHERE "ToUrl" = 'https://a.example/keep'""");
        ((string)keep.AnchorText).Should().Be("new");
        ((DateTimeOffset)keep.FirstSeenAt).Should().Be(TestData.Now);
        var added = await PostgresTestData.Row(postgres.DataSource, """SELECT * FROM "Links" WHERE "ToUrl" = 'https://a.example/added'""");
        ((DateTimeOffset)added.FirstSeenAt).Should().Be(TestData.Now.AddHours(1));

        await Parse(home, []);

        (await StoredTargets(home)).Should().BeEmpty();
    }

    [Fact]
    public async Task TheTargetsOfOnePageAreUniqueAndLongUrlsWorkWithTheMd5Index()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var home = await Page(job, "https://a.example/");
        var longUrl = "https://a.example/" + new string('p', PageLinkPreparer.MaxUrlLength - 18);
        var longOther = longUrl[..^1] + "q";

        await Parse(home, [Link(longUrl), Link(longOther), Link(longUrl)]);

        (await StoredTargets(home)).Should().Equal(longUrl, longOther);
    }

    [Fact]
    public async Task ATargetOverTheLimitIsSkippedByThePreparer()
    {
        var page = new Uri("https://a.example/");
        var links = PageLinkPreparer.Prepare(
            page,
            page,
            [new ParsedLink { Url = "/" + new string('a', PageLinkPreparer.MaxUrlLength) }, new ParsedLink { Url = "/ok" }],
            _ => false);
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var home = await Page(job, "https://a.example/");

        await Parse(home, links);

        (await StoredTargets(home)).Should().Equal("https://a.example/ok");
    }

    [Fact]
    public async Task TheDetailCountsTheLinksAndFoundOnIsTheEarliestLinkToThePage()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var home = await Page(job, "https://a.example/");
        var other = await Page(job, "https://a.example/other");
        var target = await Page(job, "https://a.example/t");
        await Parse(other, [Link("https://a.example/t", "from other")]);
        _time.Advance(TimeSpan.FromMinutes(5));
        await Parse(home, [Link("https://a.example/t", "from home"), Link("https://b.example/", internalLink: false), Link("https://a.example/z")]);
        await Parse(target, []);

        var detail = await Pages.FindPage(home, default);
        var targetDetail = await Pages.FindPage(target, default);
        var homeDetail = detail!;

        homeDetail.Parsed!.LinkCount.Should().Be(3);
        homeDetail.Parsed.InternalLinkCount.Should().Be(2);
        homeDetail.FoundOn.Should().BeNull();
        targetDetail!.FoundOn.Should().Be(new FoundOn(other, "https://a.example/other"));
        targetDetail.Parsed!.LinkCount.Should().Be(0);
    }

    [Fact]
    public async Task TheOutAndInListsPageAndCarryTheStatusOfKnownPages()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var home = await Page(job, "https://a.example/");
        var other = await Page(job, "https://a.example/other");
        var target = await Page(job, "https://a.example/t");
        await Parse(home, [Link("https://a.example/t", "T"), Link("https://b.example/", "B", internalLink: false), Link("https://a.example/other")]);
        _time.Advance(TimeSpan.FromMinutes(1));
        await Parse(other, [Link("https://a.example/t", "T2", noFollow: true), Link("https://a.example/")]);

        var firstOut = await Pages.ListLinks(home, new LinkQuery(LinkDirection.Out, 2, 0), default);
        var secondOut = await Pages.ListLinks(home, new LinkQuery(LinkDirection.Out, 2, 2), default);
        var incoming = await Pages.ListLinks(target, new LinkQuery(LinkDirection.In, 1, 0), default);
        var incomingNext = await Pages.ListLinks(target, new LinkQuery(LinkDirection.In, 1, 1), default);

        firstOut!.Total.Should().Be(3);
        firstOut.Items.Should().Equal(
            new PageLinkItem("https://a.example/t", "T", true, false, target, 200),
            new PageLinkItem("https://b.example/", "B", false, false, null, null));
        secondOut!.Items.Should().Equal(new PageLinkItem("https://a.example/other", null, true, false, other, 200));
        incoming!.Total.Should().Be(2);
        incoming.Items.Should().Equal(new PageLinkItem("https://a.example/", "T", true, false, home, 200));
        incomingNext!.Items.Should().Equal(new PageLinkItem("https://a.example/other", "T2", true, true, other, 200));
    }

    [Fact]
    public async Task TheLinksOfAnUnknownPageAreNull()
    {
        (await Pages.ListLinks(999, new LinkQuery(LinkDirection.Out, 10, 0), default)).Should().BeNull();
    }

    [Fact]
    public async Task DeletingAPageDeletesItsLinks()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var home = await Page(job, "https://a.example/");
        await Parse(home, [Link("https://a.example/x")]);

        await PostgresTestData.Scalar<int>(postgres.DataSource, """DELETE FROM "DiscoveredUrls" WHERE "Id" = @home RETURNING "Id" """, new { home });

        (await PostgresTestData.Scalar<long>(postgres.DataSource, """SELECT count(*) FROM "Links" """)).Should().Be(0);
    }
}
