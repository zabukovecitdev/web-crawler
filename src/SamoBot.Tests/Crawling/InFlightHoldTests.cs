using FluentAssertions;
using SamoBot.Core.Crawling;

namespace SamoBot.Tests.Crawling;

public class InFlightHoldTests
{
    private static readonly TimeSpan Cap = TimeSpan.FromMinutes(5);

    [Theory]
    [InlineData(20, 45, 5, 50)]
    [InlineData(60, 45, 5, 65)]
    [InlineData(20, 20, 5, 25)]
    [InlineData(10, 45, 0, 45)]
    [InlineData(10, 45, -3, 45)]
    [InlineData(20, 45, 10, 55)]
    [InlineData(280, 45, 30, 300)]
    public void TheHoldIsTheLongestFetchPlusTheMarginAndNeverMoreThanTheCap(int requestSeconds, int browserSeconds, int marginSeconds, int expectedSeconds)
    {
        var hold = InFlightHold.Calculate(
            TimeSpan.FromSeconds(requestSeconds), TimeSpan.FromSeconds(browserSeconds), TimeSpan.FromSeconds(marginSeconds), Cap);

        hold.Duration.Should().Be(TimeSpan.FromSeconds(expectedSeconds));
    }

    [Theory]
    [InlineData(0, 45)]
    [InlineData(20, 0)]
    [InlineData(-1, 45)]
    public void ADeadlineThatIsTurnedOffGivesTheCap(int requestSeconds, int browserSeconds) =>
        InFlightHold.Calculate(TimeSpan.FromSeconds(requestSeconds), TimeSpan.FromSeconds(browserSeconds), TimeSpan.FromSeconds(5), Cap)
            .Duration.Should().Be(Cap);
}
