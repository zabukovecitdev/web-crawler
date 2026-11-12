using System.Net;
using FluentAssertions;
using SamoBot.Adapters.Http;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Http;

public class HttpRobotsSourceTests
{
    private static (HttpRobotsSource Source, StubHttpHandler Handler) Build(Func<HttpRequestMessage, HttpResponseMessage> respond)
    {
        var handler = new StubHttpHandler(respond);
        return (new HttpRobotsSource(new HttpClient(handler), TimeProvider.System), handler);
    }

    private static HttpResponseMessage Status(HttpStatusCode code, string body = "") => new(code) { Content = new StringContent(body) };

    [Fact]
    public async Task Download_OfAFoundFile_ParsesTheRulesAndExpiresAfterOneDay()
    {
        var (source, handler) = Build(_ => Status(HttpStatusCode.OK, "User-agent: *\nDisallow: /private\n"));

        var rules = await source.Download("a.example", default);

        rules.IsAllowed("/private", "SamoBot").Should().BeFalse();
        rules.IsAllowed("/open", "SamoBot").Should().BeTrue();
        (rules.ExpiresAt - rules.FetchedAt).Should().Be(TimeSpan.FromHours(24));
        handler.Requests.Select(r => r.RequestUri!.ToString()).Should().Equal("https://a.example/robots.txt");
    }

    [Theory]
    [InlineData(HttpStatusCode.NotFound)]
    [InlineData(HttpStatusCode.Forbidden)]
    [InlineData(HttpStatusCode.Gone)]
    public async Task Download_OfAMissingFile_AllowsEverythingForOneDay(HttpStatusCode code)
    {
        var (source, _) = Build(_ => Status(code));

        var rules = await source.Download("a.example", default);

        rules.IsAllowed("/anything", "SamoBot").Should().BeTrue();
        (rules.ExpiresAt - rules.FetchedAt).Should().Be(TimeSpan.FromHours(24));
    }

    [Theory]
    [InlineData(HttpStatusCode.InternalServerError)]
    [InlineData(HttpStatusCode.ServiceUnavailable)]
    public async Task Download_OfAServerError_BlocksEverythingForOneHour(HttpStatusCode code)
    {
        var (source, _) = Build(_ => Status(code));

        var rules = await source.Download("a.example", default);

        rules.IsAllowed("/anything", "SamoBot").Should().BeFalse();
        (rules.ExpiresAt - rules.FetchedAt).Should().Be(TimeSpan.FromHours(1));
    }

    [Fact]
    public async Task Download_WhenBothSchemesFailToConnect_BlocksEverythingForOneHour()
    {
        var (source, handler) = Build(_ => throw new HttpRequestException("refused"));

        var rules = await source.Download("a.example", default);

        rules.IsAllowed("/anything", "SamoBot").Should().BeFalse();
        (rules.ExpiresAt - rules.FetchedAt).Should().Be(TimeSpan.FromHours(1));
        handler.Requests.Should().HaveCount(2);
    }

    [Fact]
    public async Task Download_WhenHttpsFails_FallsBackToHttpAndKeepsThePort()
    {
        var (source, handler) = Build(request => request.RequestUri!.Scheme == "https"
            ? throw new HttpRequestException("refused")
            : Status(HttpStatusCode.OK, "User-agent: *\nDisallow: /x\n"));

        var rules = await source.Download("localhost:5000", default);

        rules.IsAllowed("/x", "SamoBot").Should().BeFalse();
        handler.Requests.Select(r => r.RequestUri!.ToString()).Should()
            .Equal("https://localhost:5000/robots.txt", "http://localhost:5000/robots.txt");
    }
}
