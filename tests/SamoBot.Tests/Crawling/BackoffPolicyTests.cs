using FluentAssertions;
using SamoBot.Core.Crawling;

namespace SamoBot.Tests.Crawling;

public class BackoffPolicyTests
{
    [Theory]
    [InlineData(0, 1)]
    [InlineData(1, 2)]
    [InlineData(2, 4)]
    [InlineData(5, 32)]
    [InlineData(6, 60)]
    [InlineData(50, 60)]
    public void DelayForDoublesEachFailureUpToOneHour(int failCount, int expectedMinutes)
    {
        BackoffPolicy.DelayFor(failCount).Should().Be(TimeSpan.FromMinutes(expectedMinutes));
    }
}
