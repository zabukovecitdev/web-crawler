using FluentAssertions;
using SamoBot.Api.Contracts;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Api;

public class AnalyticsQueriesTests
{
    [Fact]
    public void DefaultsToAllRangeDayBucketAndNoHost() =>
        AnalyticsQueries.Parse(null, null, null).Should().Be(new AnalyticsQuery(AnalyticsRange.All, null, AnalyticsBucket.Day));

    [Fact]
    public void HostIsTrimmedAndLowercased() =>
        AnalyticsQueries.Parse("24h", "  Example.COM ", null)
            .Should().Be(new AnalyticsQuery(AnalyticsRange.Day, "example.com", AnalyticsBucket.Hour));

    [Fact]
    public void BlankHostMeansAllHosts() => AnalyticsQueries.Parse("7d", "  ", "day")!.Host.Should().BeNull();

    [Theory]
    [InlineData("1y", null)]
    [InlineData("7d", "month")]
    public void UnknownRangeOrBucketIsRejected(string range, string? bucket) =>
        AnalyticsQueries.Parse(range, null, bucket).Should().BeNull();
}
