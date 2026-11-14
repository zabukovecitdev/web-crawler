using System.Net;
using FluentAssertions;
using SamoBot.Adapters.Http;
using SamoBot.Core.Sitemaps;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Http;

public class HttpSitemapSourceTests
{
    private static HttpSitemapSource Build(Func<HttpRequestMessage, HttpResponseMessage> respond) =>
        new(new HttpClient(new StubHttpHandler(respond)));

    [Fact]
    public async Task DownloadReturnsTheBodyOfASuccessfulResponse()
    {
        var source = Build(_ => new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent([1, 2, 3]) });

        var download = await source.Download("https://a.example/sitemap.xml", default);

        download.IsSuccess.Should().BeTrue();
        download.Content.Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task DownloadOfAnErrorStatusKeepsTheStatusAndNoBody()
    {
        var download = await Build(_ => new HttpResponseMessage(HttpStatusCode.NotFound)).Download("https://a.example/s.xml", default);

        download.Should().Match<SamoBot.Core.Domain.SitemapDownload>(d => d.StatusCode == 404 && d.Content == null && !d.IsSuccess);
    }

    [Fact]
    public async Task DownloadOfABodyOverTenMegabytesIsAFailure()
    {
        var big = new byte[SitemapLimits.MaxCompressedBytes + 10];
        var source = Build(_ => new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(new MemoryStream(big)) });

        var download = await source.Download("https://a.example/s.xml", default);

        download.IsSuccess.Should().BeFalse();
        download.Content.Should().BeNull();
    }

    [Fact]
    public async Task DownloadOfARedirectIsNotFollowedAndGivesNoBody()
    {
        var source = Build(_ => new HttpResponseMessage(HttpStatusCode.Found));

        var download = await source.Download("https://a.example/s.xml", default);

        download.IsSuccess.Should().BeFalse();
    }

    [Fact]
    public async Task DownloadOfANetworkErrorGivesStatusZeroInsteadOfThrowing()
    {
        var source = Build(_ => throw new HttpRequestException("down"));

        var download = await source.Download("https://a.example/s.xml", default);

        download.StatusCode.Should().Be(0);
    }
}
