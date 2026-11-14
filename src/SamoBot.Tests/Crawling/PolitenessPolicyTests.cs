using FluentAssertions;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Crawling;

public class PolitenessPolicyTests
{
    private static readonly PolitenessPolicy Policy = new(TimeSpan.FromSeconds(60));

    private static ClaimedUrl Host(int baseMs = 1000, double? avg = null, int failures = 0) =>
        TestData.Url() with { CrawlDelayMs = baseMs, AvgResponseMs = avg, ConsecutiveFailures = failures };

    private static FetchResponse Response(int status, long ms = 100, TimeSpan? retryAfter = null) => new()
    {
        StatusCode = status,
        ResponseTimeMs = ms,
        RetryAfter = retryAfter
    };

    private static double Seconds(HostPacingUpdate? update) => update!.Delay.TotalSeconds;

    [Theory]
    [InlineData(1000, null, 100, 1)]
    [InlineData(1000, null, 1000, 5)]
    [InlineData(1000, 1000.0, 2000, 6.5)]
    [InlineData(1000, 100.0, 100, 1)]
    [InlineData(10_000, null, 1000, 10)]
    [InlineData(1000, null, 100_000, 60)]
    [InlineData(70_000, null, 100_000, 70)]
    public void ASuccessPacesTheHostByTheBaseDelayOrFiveTimesTheAverage(int baseMs, double? avg, long ms, double expectedSeconds)
    {
        var update = Policy.Next(Host(baseMs, avg), Response(200, ms));

        Seconds(update).Should().BeApproximately(expectedSeconds, 0.001);
        update!.ConsecutiveFailures.Should().Be(0);
    }

    [Theory]
    [InlineData(null, 100, 100.0)]
    [InlineData(1000.0, 2000, 1300.0)]
    [InlineData(500.0, 0, 350.0)]
    public void TheAverageIsAMovingAverageWithAlphaZeroPointThree(double? previous, long ms, double expected)
    {
        Policy.Next(Host(avg: previous), Response(200, ms))!.AvgResponseMs.Should().BeApproximately(expected, 0.001);
    }

    [Theory]
    [InlineData(404)]
    [InlineData(301)]
    [InlineData(410)]
    public void AnAnswerBelow500IsNotAFailure(int status)
    {
        var update = Policy.Next(Host(failures: 3), Response(status, 200));

        update.Should().Be(new HostPacingUpdate(TimeSpan.FromSeconds(1), 200, 0));
    }

    [Theory]
    [InlineData(0, 2)]
    [InlineData(1, 4)]
    [InlineData(2, 8)]
    [InlineData(4, 32)]
    [InlineData(5, 60)]
    [InlineData(30, 60)]
    public void EachConsecutiveTimeoutDoublesTheDelayUpToTheCap(int failuresBefore, double expectedSeconds)
    {
        var update = Policy.Next(Host(failures: failuresBefore), Response(0, 20_000));

        Seconds(update).Should().Be(expectedSeconds);
        update!.ConsecutiveFailures.Should().Be(failuresBefore + 1);
    }

    [Theory]
    [InlineData(500)]
    [InlineData(502)]
    [InlineData(504)]
    public void AServerErrorDoublesTheSlowHostDelayAndKeepsTheAverage(int status)
    {
        var update = Policy.Next(Host(avg: 1000, failures: 0), Response(status, 5000));

        update.Should().Be(new HostPacingUpdate(TimeSpan.FromSeconds(10), 1000, 1));
    }

    [Fact]
    public void TheBaseDelayIsTheMinimumEvenWhenItIsAboveTheAdaptiveCap()
    {
        Seconds(Policy.Next(Host(baseMs: 90_000, failures: 4), Response(0))).Should().Be(90);
    }

    [Fact]
    public void ASuccessAfterFailuresResetsTheCountAndTheDelay()
    {
        var update = Policy.Next(Host(failures: 6, avg: 100), Response(200, 100));

        update.Should().Be(new HostPacingUpdate(TimeSpan.FromSeconds(1), 100, 0));
    }

    [Theory]
    [InlineData(429, 0, 2)]
    [InlineData(503, 4, 32)]
    [InlineData(429, 9, 600)]
    [InlineData(503, 20, 600)]
    public void ARateLimitWithoutRetryAfterBacksOffFromTheBaseDelayUpToTenMinutes(int status, int failuresBefore, double expectedSeconds)
    {
        Seconds(Policy.Next(Host(failures: failuresBefore), Response(status))).Should().Be(expectedSeconds);
    }

    [Theory]
    [InlineData(429, 30, 30)]
    [InlineData(503, 120, 120)]
    [InlineData(503, 7200, 3600)]
    [InlineData(429, 0, 2)]
    [InlineData(429, 1, 2)]
    public void ARetryAfterIsAMinimumWaitCappedAtOneHourAndNotByTheAdaptiveCap(int status, int retryAfterSeconds, double expectedSeconds)
    {
        var update = Policy.Next(Host(), Response(status, retryAfter: TimeSpan.FromSeconds(retryAfterSeconds)));

        Seconds(update).Should().Be(expectedSeconds);
    }

    [Fact]
    public void ARetryAfterNeverShortensTheBackoffOfRepeatedFailures()
    {
        var update = Policy.Next(Host(failures: 4), Response(429, retryAfter: TimeSpan.FromSeconds(5)));

        Seconds(update).Should().Be(32);
    }

    [Fact]
    public void ARetryAfterOnAnotherStatusIsIgnored()
    {
        Seconds(Policy.Next(Host(), Response(500, retryAfter: TimeSpan.FromMinutes(30)))).Should().Be(2);
    }

    [Fact]
    public void ARetryAfterAboveTheCapRaisesTheRecordedDelayAndFailureCount()
    {
        var update = Policy.Next(Host(failures: 1), Response(429, retryAfter: TimeSpan.FromMinutes(20)));

        update.Should().Be(new HostPacingUpdate(TimeSpan.FromMinutes(20), null, 2));
    }

    [Fact]
    public void ABlockedAddressChangesNothing()
    {
        Policy.Next(Host(failures: 2, avg: 300), FetchResponse.BlockedAddress(40)).Should().BeNull();
    }

    [Fact]
    public void ACustomCapLimitsTheAdaptivePart()
    {
        var policy = new PolitenessPolicy(TimeSpan.FromSeconds(5));

        Seconds(policy.Next(Host(), Response(200, 10_000))).Should().Be(5);
    }
}
