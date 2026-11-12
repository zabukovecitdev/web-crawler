using System.Net;
using FluentAssertions;
using Microsoft.Extensions.Options;
using SamoBot.Adapters.Browser;
using SamoBot.Adapters.Http;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Http;

public class HttpFetcherTests
{
    private static HttpFetcher Fetcher(Func<HttpRequestMessage, HttpResponseMessage> respond, bool chromeEnabled = false) =>
        new(
            new HttpClient(new StubHttpHandler(respond)),
            Options.Create(new ChromeRenderingOptions { Enabled = chromeEnabled, CdpEndpoint = chromeEnabled ? "http://chrome:9222" : null }),
            TimeProvider.System);

    private static HttpResponseMessage Html(HttpStatusCode status, string body = "<html></html>") => new(status)
    {
        Content = new StringContent(body, System.Text.Encoding.UTF8, "text/html")
    };

    [Fact]
    public async Task Fetch_ReturnsStatusContentAndContentType()
    {
        var response = await Fetcher(_ => Html(HttpStatusCode.OK, "<p>hi</p>")).Fetch(TestData.Url(), default);

        response.StatusCode.Should().Be(200);
        response.ContentType.Should().StartWith("text/html");
        System.Text.Encoding.UTF8.GetString(response.Content!).Should().Be("<p>hi</p>");
        response.IsHtml.Should().BeTrue();
    }

    [Fact]
    public async Task Fetch_ReportsTheLocationOfARedirectAndDoesNotThrow()
    {
        var response = await Fetcher(_ =>
        {
            var redirect = new HttpResponseMessage(HttpStatusCode.MovedPermanently);
            redirect.Headers.Location = new Uri("/new", UriKind.Relative);
            return redirect;
        }).Fetch(TestData.Url(), default);

        response.StatusCode.Should().Be(301);
        response.RedirectLocation.Should().Be("/new");
    }

    [Fact]
    public async Task Fetch_OfAServerError_ReturnsTheStatusWithoutThrowing()
    {
        var response = await Fetcher(_ => Html(HttpStatusCode.InternalServerError, string.Empty)).Fetch(TestData.Url(), default);

        response.StatusCode.Should().Be(500);
        response.Error.Should().BeNull();
    }

    [Fact]
    public async Task Fetch_OfANetworkError_ReturnsStatusZeroWithTheError()
    {
        var response = await Fetcher(_ => throw new HttpRequestException("connection refused")).Fetch(TestData.Url(), default);

        response.StatusCode.Should().Be(0);
        response.Error.Should().Be("connection refused");
    }

    [Fact]
    public async Task Fetch_OfATimeout_ReturnsStatusZeroWithTimeout()
    {
        var response = await Fetcher(_ => throw new TaskCanceledException("timed out")).Fetch(TestData.Url(), default);

        response.StatusCode.Should().Be(0);
        response.Error.Should().Be("timeout");
    }

    [Fact]
    public async Task Fetch_StopsReadingAfterTheBodyLimit()
    {
        var big = new string('x', HttpFetcher.MaxBodyBytes + 100_000);

        var response = await Fetcher(_ => Html(HttpStatusCode.OK, big)).Fetch(TestData.Url(), default);

        response.Content.Should().HaveCount(HttpFetcher.MaxBodyBytes);
    }

    [Fact]
    public async Task Fetch_WhenTheCallerCancels_Throws()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var fetcher = Fetcher(_ => throw new TaskCanceledException("canceled", null, cts.Token));

        var act = () => fetcher.Fetch(TestData.Url(), cts.Token);

        await act.Should().ThrowAsync<TaskCanceledException>();
    }

    [Theory]
    [InlineData(false, false, true)]
    [InlineData(true, false, true)]
    [InlineData(false, true, true)]
    [InlineData(true, true, false)]
    public void Supports_UsesTheBrowserOnlyForJsUrlsWhenRenderingIsEnabled(bool useJs, bool chromeEnabled, bool expected)
    {
        var fetcher = Fetcher(_ => Html(HttpStatusCode.OK), chromeEnabled);

        fetcher.Supports(TestData.Url(useJsRendering: useJs)).Should().Be(expected);
    }
}
