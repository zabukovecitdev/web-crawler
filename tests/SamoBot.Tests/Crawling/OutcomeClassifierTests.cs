using FluentAssertions;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Crawling;

public class OutcomeClassifierTests
{
    [Theory]
    [InlineData(200)]
    [InlineData(204)]
    [InlineData(299)]
    public void Classify_Status2xx_ReturnsSucceededWithOneDay(int status)
    {
        OutcomeClassifier.Classify(TestData.Response(status), 3).Should().Be(new Succeeded(TimeSpan.FromDays(1)));
    }

    [Theory]
    [InlineData(301)]
    [InlineData(302)]
    [InlineData(303)]
    [InlineData(307)]
    [InlineData(308)]
    public void Classify_RedirectWithLocation_ReturnsRedirected(int status)
    {
        OutcomeClassifier.Classify(TestData.Response(status, "/next"), 0).Should().Be(new Redirected("/next"));
    }

    [Fact]
    public void Classify_RedirectWithoutLocation_ReturnsDisabledWithHttpStatus()
    {
        OutcomeClassifier.Classify(TestData.Response(301), 0).Should().Be(new Disabled("http-301"));
    }

    [Theory]
    [InlineData(429, "http-429")]
    [InlineData(500, "http-500")]
    [InlineData(503, "http-503")]
    [InlineData(599, "http-599")]
    public void Classify_RetryableStatusBelowLimit_ReturnsRetryLaterWithBackoff(int status, string reason)
    {
        var outcome = OutcomeClassifier.Classify(TestData.Response(status), 2);

        outcome.Should().Be(new RetryLater(reason, TimeSpan.FromMinutes(4)));
    }

    [Fact]
    public void Classify_NetworkError_ReturnsRetryLaterWithErrorText()
    {
        var response = new FetchResponse { StatusCode = 0, Error = "timeout" };

        OutcomeClassifier.Classify(response, 0).Should().Be(new RetryLater("timeout", TimeSpan.FromMinutes(1)));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(429)]
    [InlineData(500)]
    public void Classify_RetryableStatusAtLimit_ReturnsDisabledMaxFailures(int status)
    {
        OutcomeClassifier.Classify(TestData.Response(status), 4).Should().Be(new Disabled("max-failures"));
    }

    [Theory]
    [InlineData(400, "http-400")]
    [InlineData(404, "http-404")]
    [InlineData(410, "http-410")]
    [InlineData(304, "http-304")]
    public void Classify_OtherStatus_ReturnsDisabledWithHttpStatus(int status, string reason)
    {
        OutcomeClassifier.Classify(TestData.Response(status), 0).Should().Be(new Disabled(reason));
    }
}
