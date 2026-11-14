using FluentAssertions;
using SamoBot.Api.Contracts;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Api;

public class LinkListQueriesTests
{
    [Theory]
    [InlineData("out", LinkDirection.Out)]
    [InlineData("IN", LinkDirection.In)]
    public void ADirectionIsParsed(string direction, LinkDirection expected)
    {
        LinkListQueries.Parse(direction, 50, 0).Query!.Direction.Should().Be(expected);
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(-5, 1)]
    [InlineData(50, 50)]
    [InlineData(1000, 200)]
    public void TheLimitIsClamped(int limit, int expected)
    {
        LinkListQueries.Parse("out", limit, 0).Query!.Limit.Should().Be(expected);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("both")]
    public void ABadDirectionIsAnError(string? direction)
    {
        LinkListQueries.Parse(direction, 50, 0).Error.Should().NotBeNull();
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(100_001)]
    public void AnOffsetOutOfRangeIsAnError(int offset)
    {
        LinkListQueries.Parse("out", 50, offset).Error.Should().Contain("Offset");
    }

    [Fact]
    public void TheLargestOffsetIsAccepted()
    {
        LinkListQueries.Parse("in", 10, ListLimits.MaxOffset).Query!.Offset.Should().Be(ListLimits.MaxOffset);
    }
}
