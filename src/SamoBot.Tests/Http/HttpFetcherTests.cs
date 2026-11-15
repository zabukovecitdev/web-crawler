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
            Options.Create(new CrawlerNetworkOptions()),
            TestDirectives.Headers,
            TimeProvider.System);

    private static HttpResponseMessage Html(HttpStatusCode status, string body = "<html></html>") => new(status)
    {
        Content = new StringContent(body, System.Text.Encoding.UTF8, "text/html")
    };

    [Fact]
    public async Task FetchReturnsStatusContentAndContentType()
    {
        var response = await Fetcher(_ => Html(HttpStatusCode.OK, "<p>hi</p>")).Fetch(TestData.Url(), default);

        response.StatusCode.Should().Be(200);
        response.ContentType.Should().StartWith("text/html");
        System.Text.Encoding.UTF8.GetString(response.Content!).Should().Be("<p>hi</p>");
        response.IsHtml.Should().BeTrue();
    }

    [Fact]
    public async Task FetchReportsTheLocationOfARedirectAndDoesNotThrow()
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
    public async Task FetchOfAServerErrorReturnsTheStatusWithoutThrowing()
    {
        var response = await Fetcher(_ => Html(HttpStatusCode.InternalServerError, string.Empty)).Fetch(TestData.Url(), default);

        response.StatusCode.Should().Be(500);
        response.Error.Should().BeNull();
    }

    [Fact]
    public async Task FetchOfANetworkErrorReturnsStatusZeroWithTheError()
    {
        var response = await Fetcher(_ => throw new HttpRequestException("connection refused")).Fetch(TestData.Url(), default);

        response.StatusCode.Should().Be(0);
        response.Error.Should().Be("connection refused");
    }

    [Fact]
    public async Task FetchOfATimeoutReturnsStatusZeroWithTimeout()
    {
        var response = await Fetcher(_ => throw new TaskCanceledException("timed out")).Fetch(TestData.Url(), default);

        response.StatusCode.Should().Be(0);
        response.Error.Should().Be("timeout");
    }

    [Fact]
    public async Task FetchStopsReadingAfterTheBodyLimit()
    {
        var big = new string('x', HttpFetcher.MaxBodyBytes + 100_000);

        var response = await Fetcher(_ => Html(HttpStatusCode.OK, big)).Fetch(TestData.Url(), default);

        response.Content.Should().HaveCount(HttpFetcher.MaxBodyBytes);
    }

    [Fact]
    public async Task FetchWhenTheCallerCancelsThrows()
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
    public void SupportsUsesTheBrowserOnlyForJsUrlsWhenRenderingIsEnabled(bool useJs, bool chromeEnabled, bool expected)
    {
        var fetcher = Fetcher(_ => Html(HttpStatusCode.OK), chromeEnabled);

        fetcher.Supports(TestData.Url(useJsRendering: useJs)).Should().Be(expected);
    }

    private static HttpFetcher FetcherAt(DateTimeOffset now, Func<HttpRequestMessage, HttpResponseMessage> respond) =>
        new(
            new HttpClient(new StubHttpHandler(respond)),
            Options.Create(new ChromeRenderingOptions()),
            Options.Create(new CrawlerNetworkOptions()),
            TestDirectives.Headers,
            new Microsoft.Extensions.Time.Testing.FakeTimeProvider(now));

    [Fact]
    public async Task FetchReadsRetryAfterInSeconds()
    {
        var response = await Fetcher(_ =>
        {
            var limited = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
            limited.Headers.RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.FromSeconds(120));
            return limited;
        }).Fetch(TestData.Url(), default);

        response.RetryAfter.Should().Be(TimeSpan.FromSeconds(120));
    }

    [Theory]
    [InlineData(90, 90)]
    [InlineData(-30, 0)]
    public async Task FetchReadsRetryAfterAsADateRelativeToNow(int secondsFromNow, int expectedSeconds)
    {
        var fetcher = FetcherAt(TestData.Now, _ =>
        {
            var limited = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
            limited.Headers.RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TestData.Now.AddSeconds(secondsFromNow));
            return limited;
        });

        var response = await fetcher.Fetch(TestData.Url(), default);

        response.RetryAfter.Should().Be(TimeSpan.FromSeconds(expectedSeconds));
    }

    [Fact]
    public async Task FetchWithoutRetryAfterHasNone()
    {
        var response = await Fetcher(_ => Html(HttpStatusCode.TooManyRequests, string.Empty)).Fetch(TestData.Url(), default);

        response.RetryAfter.Should().BeNull();
    }

    [Fact]
    public async Task FetchKeepsTheHeadersTheDirectivesReadWholeAndDropsTheRest()
    {
        var response = await Fetcher(_ =>
        {
            var page = Html(HttpStatusCode.OK);
            page.Headers.TryAddWithoutValidation("X-Robots-Tag", "noindex");
            page.Headers.TryAddWithoutValidation("X-Robots-Tag", "googlebot: noindex, nofollow");
            page.Headers.TryAddWithoutValidation("Link", "<https://a.example/main>; rel=\"canonical\"");
            page.Headers.TryAddWithoutValidation("Set-Cookie", "session=secret");
            return page;
        }).Fetch(TestData.Url(), default);

        response.ResponseHeaders.Values("x-robots-tag").Should().Equal("noindex", "googlebot: noindex, nofollow");
        response.ResponseHeaders.Values("Link").Should().Equal("<https://a.example/main>; rel=\"canonical\"");
        response.ResponseHeaders.Values("Set-Cookie").Should().BeEmpty();
    }

    [Fact]
    public async Task FetchWithoutDirectiveHeadersKeepsNone()
    {
        var response = await Fetcher(_ => Html(HttpStatusCode.OK)).Fetch(TestData.Url(), default);

        response.ResponseHeaders.IsEmpty.Should().BeTrue();
    }
}
