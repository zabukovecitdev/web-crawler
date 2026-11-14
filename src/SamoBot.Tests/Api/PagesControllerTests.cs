using FluentAssertions;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Api.Controllers;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Api;

public class PagesControllerTests
{
    private sealed class FakePageReader(string? objectName, PagedResult<PageLinkItem>? links = null) : IPageReader
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

        public Task<string?> FindContentObjectName(int id, CancellationToken ct) => Task.FromResult(objectName);
    }

    private static PagesController Controller(string? objectName, FakeContentStore store, FakePageReader? reader = null) =>
        new(reader ?? new FakePageReader(objectName), store)
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
    public async Task ContentWithoutAStoredBodyIsNotFound()
    {
        var result = await Controller(null, new FakeContentStore()).GetContent(7, default);

        result.Should().BeOfType<NotFoundResult>();
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
