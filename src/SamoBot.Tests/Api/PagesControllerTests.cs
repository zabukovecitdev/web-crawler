using FluentAssertions;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Api.Controllers;
using SamoBot.Core.Directives;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Api;

public class PagesControllerTests
{
    private sealed class FakePageReader(string? objectName, PagedResult<PageLinkItem>? links = null, PagePolicy? policy = null) : IPageReader
    {
        public LinkQuery? LastLinkQuery { get; private set; }

        public Task<PagedResult<PageLinkItem>?> ListLinks(int id, LinkQuery query, CancellationToken ct)
        {
            LastLinkQuery = query;
            return Task.FromResult(links);
        }

        public Task<PagedResult<PageSummary>> ListHostPages(string host, PageQuery query, CancellationToken ct) =>
            throw new NotSupportedException();

        public Task<PageDetail?> FindPage(int id, CancellationToken ct) => throw new NotSupportedException();

        public Task<StoredCopy?> FindStoredCopy(int id, CancellationToken ct) =>
            Task.FromResult(objectName is null ? null : new StoredCopy(objectName, new Uri("https://example.com/page"), policy ?? PagePolicy.Default));
    }

    private static PagesController Controller(
        string? objectName, FakeContentStore store, FakePageReader? reader = null, FakeMarkdownConverter? converter = null) =>
        new(reader ?? new FakePageReader(objectName), store, converter ?? new FakeMarkdownConverter())
    {
        ControllerContext = new ControllerContext { HttpContext = new DefaultHttpContext() }
    };

    [Fact]
    public async Task ContentIsServedWithSniffingAndActiveContentDisabled()
    {
        var store = new FakeContentStore();
        store.Put("obj", "<script>alert(1)</script>");
        var controller = Controller("obj", store);

        var result = await controller.GetContent(7, default);

        result.Should().BeOfType<FileContentResult>();
        var headers = controller.Response.Headers;
        headers["X-Content-Type-Options"].ToString().Should().Be("nosniff");
        headers["Content-Security-Policy"].ToString().Should().Be("default-src 'none'; sandbox");
        headers["Content-Disposition"].ToString().Should().StartWith("inline").And.Contain("page-7.txt");
        headers[StoredContent.TruncatedHeader].ToString().Should().Be("false");
    }

    [Fact]
    public async Task ContentOfANoArchivePageIsForbiddenAndNotLoaded()
    {
        var store = new FakeContentStore();
        var reader = new FakePageReader("obj", policy: PagePolicy.Default with { Archive = false });

        var result = await Controller("obj", store, reader).GetContent(7, default);

        var refused = result.Should().BeOfType<ObjectResult>().Subject;
        refused.StatusCode.Should().Be(StatusCodes.Status403Forbidden);
        refused.Value.Should().BeOfType<ProblemDetails>().Which.Title.Should().Be(PagesController.NoArchiveMessage);
    }

    [Fact]
    public async Task ContentWithoutAStoredBodyIsNotFound()
    {
        var result = await Controller(null, new FakeContentStore()).GetContent(7, default);

        result.Should().BeOfType<NotFoundResult>();
    }

    [Fact]
    public async Task MarkdownWithoutAStoredBodyIsNotFound()
    {
        var result = await Controller(null, new FakeContentStore()).GetMarkdown(7, default);

        result.Should().BeOfType<NotFoundResult>();
    }

    [Fact]
    public async Task MarkdownOfANoArchivePageIsForbiddenAndNotConverted()
    {
        var converter = new FakeMarkdownConverter();
        var reader = new FakePageReader("obj", policy: PagePolicy.Default with { Archive = false });

        var result = await Controller("obj", new FakeContentStore(), reader, converter).GetMarkdown(7, default);

        var refused = result.Should().BeOfType<ObjectResult>().Subject;
        refused.StatusCode.Should().Be(StatusCodes.Status403Forbidden);
        refused.Value.Should().BeOfType<ProblemDetails>().Which.Title.Should().Be(PagesController.NoArchiveMessage);
        converter.Calls.Should().BeEmpty();
    }

    [Fact]
    public async Task MarkdownIsConvertedFromTheStoredHtmlAndServedWithSniffingAndActiveContentDisabled()
    {
        var store = new FakeContentStore();
        store.Put("obj", "<h1>Hi</h1>");
        var converter = new FakeMarkdownConverter("# Hi\n");
        var controller = Controller("obj", store, converter: converter);

        var result = await controller.GetMarkdown(7, default);

        var file = result.Should().BeOfType<FileContentResult>().Subject;
        file.ContentType.Should().Be("text/markdown; charset=utf-8");
        System.Text.Encoding.UTF8.GetString(file.FileContents).Should().Be("# Hi\n");
        converter.Calls.Should().Equal(("<h1>Hi</h1>", new Uri("https://example.com/page")));
        var headers = controller.Response.Headers;
        headers["X-Content-Type-Options"].ToString().Should().Be("nosniff");
        headers["Content-Security-Policy"].ToString().Should().Be("default-src 'none'; sandbox");
        headers["Content-Disposition"].ToString().Should().StartWith("inline").And.Contain("page-7.md");
        headers[StoredContent.TruncatedHeader].ToString().Should().Be("false");
    }

    [Fact]
    public async Task MarkdownOfAnOversizedBodyConvertsTheCappedHtmlAndSaysSo()
    {
        var store = new FakeContentStore();
        store.Put("obj", new string('a', StoredContent.MaxBytes + 10));
        var converter = new FakeMarkdownConverter();
        var controller = Controller("obj", store, converter: converter);

        await controller.GetMarkdown(7, default);

        controller.Response.Headers[StoredContent.TruncatedHeader].ToString().Should().Be("true");
        converter.Calls.Single().Html.Length.Should().Be(StoredContent.MaxBytes);
    }

    [Theory]
    [InlineData("sideways", 50, 0)]
    [InlineData("", 50, 0)]
    [InlineData("out", 50, -1)]
    [InlineData("in", 50, 100_001)]
    public async Task LinksWithABadQueryAreRejected(string direction, int limit, int offset)
    {
        var result = await Controller(null, new FakeContentStore()).GetLinks(7, direction, limit, offset, default);

        result.Should().BeOfType<BadRequestObjectResult>();
    }

    [Fact]
    public async Task LinksOfAnUnknownPageAreNotFound()
    {
        var result = await Controller(null, new FakeContentStore()).GetLinks(7, "out", 50, 0, default);

        result.Should().BeOfType<NotFoundResult>();
    }

    [Fact]
    public async Task LinksAreReturnedWithTheParsedQuery()
    {
        var page = new PagedResult<PageLinkItem>(1, [new PageLinkItem("https://a.example/", "A", true, false, 3, 200)]);
        var reader = new FakePageReader(null, page);

        var result = await Controller(null, new FakeContentStore(), reader).GetLinks(7, "IN", 500, 10, default);

        result.Should().BeOfType<OkObjectResult>().Which.Value.Should().Be(page);
        reader.LastLinkQuery.Should().Be(new LinkQuery(LinkDirection.In, 200, 10));
    }
}
