using FluentAssertions;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Domain;

public class AnalyticsWindowTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 10, 12, 0, 0, TimeSpan.Zero);

    [Theory]
    [InlineData(null, AnalyticsRange.All)]
    [InlineData("", AnalyticsRange.All)]
    [InlineData("all", AnalyticsRange.All)]
    [InlineData("24h", AnalyticsRange.Day)]
    [InlineData("7D", AnalyticsRange.Week)]
    [InlineData("30d", AnalyticsRange.Month)]
    public void RangeNamesParse(string? name, AnalyticsRange expected) =>
        AnalyticsNames.ParseRange(name).Should().Be(expected);

    [Theory]
    [InlineData("1h")]
    [InlineData("week")]
    [InlineData("'; drop table")]
    public void UnknownRangeIsRejected(string name) => AnalyticsNames.ParseRange(name).Should().BeNull();

    [Theory]
    [InlineData(null, AnalyticsRange.Day, AnalyticsBucket.Hour)]
    [InlineData(null, AnalyticsRange.Week, AnalyticsBucket.Day)]
    [InlineData(null, AnalyticsRange.All, AnalyticsBucket.Day)]
    [InlineData("hour", AnalyticsRange.Month, AnalyticsBucket.Hour)]
    [InlineData("DAY", AnalyticsRange.Day, AnalyticsBucket.Day)]
    public void BucketDefaultsFromTheRange(string? name, AnalyticsRange range, AnalyticsBucket expected) =>
        AnalyticsNames.ParseBucket(name, range).Should().Be(expected);

    [Fact]
    public void UnknownBucketIsRejected() => AnalyticsNames.ParseBucket("week", AnalyticsRange.All).Should().BeNull();

    [Theory]
    [InlineData(AnalyticsRange.Day, -24)]
    [InlineData(AnalyticsRange.Week, -168)]
    [InlineData(AnalyticsRange.Month, -720)]
    public void WindowStartsBeforeNow(AnalyticsRange range, int hours)
    {
        var window = AnalyticsWindow.For(range, Now);

        window.From.Should().Be(Now.AddHours(hours));
        window.To.Should().Be(Now);
    }

    [Fact]
    public void AllRangeHasNoLowerBound() => AnalyticsWindow.For(AnalyticsRange.All, Now).From.Should().BeNull();

    [Fact]
    public void FloorLimitsTheBucketCount()
    {
        AnalyticsNames.Floor(Now, AnalyticsBucket.Hour).Should().Be(Now.AddHours(-1000));
        AnalyticsNames.Floor(Now, AnalyticsBucket.Day).Should().Be(Now.AddDays(-1000));
    }

    [Theory]
    [InlineData("text/html", true)]
    [InlineData("TEXT/HTML", true)]
    [InlineData("application/xhtml+xml", true)]
    [InlineData("application/pdf", false)]
    [InlineData("unknown", false)]
    [InlineData(null, false)]
    public void OnlyHtmlIsStored(string? mediaType, bool expected) => StoredMediaTypes.IsStored(mediaType).Should().Be(expected);
}
