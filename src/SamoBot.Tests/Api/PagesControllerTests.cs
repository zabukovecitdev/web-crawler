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
    private sealed class FakePageReader(string? objectName) : IPageReader
    {
        public Task<PagedResult<PageSummary>> ListHostPages(string host, PageQuery query, CancellationToken ct) =>
            throw new NotSupportedException();

        public Task<PageDetail?> FindPage(int id, CancellationToken ct) => throw new NotSupportedException();

        public Task<string?> FindContentObjectName(int id, CancellationToken ct) => Task.FromResult(objectName);
    }

    private static PagesController Controller(string? objectName, FakeContentStore store) => new(new FakePageReader(objectName), store)
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
}
