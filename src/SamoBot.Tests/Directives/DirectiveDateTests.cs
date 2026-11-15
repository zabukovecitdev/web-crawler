using FluentAssertions;
using SamoBot.Core.Directives.Rules;

namespace SamoBot.Tests.Directives;

public class DirectiveDateTests
{
    [Theory]
    [InlineData("2026-12-31", "2026-12-31T00:00:00+00:00")]
    [InlineData("2026-12-31T10:30:00Z", "2026-12-31T10:30:00+00:00")]
    [InlineData("2026-12-31T10:30:00+02:00", "2026-12-31T10:30:00+02:00")]
    [InlineData("25 Jun 2010 15:00:00 PST", "2010-06-25T15:00:00-08:00")]
    [InlineData("Fri, 25 Jun 2010 15:00:00 GMT", "2010-06-25T15:00:00+00:00")]
    [InlineData("Friday, 25-Jun-10 15:00:00 PST", "2010-06-25T15:00:00-08:00")]
    [InlineData("25 June 2010", "2010-06-25T00:00:00+00:00")]
    public void ReadsTheCommonFormats(string text, string expected)
    {
        DirectiveDate.TryParse(text, out var value).Should().BeTrue();
        value.Should().Be(DateTimeOffset.Parse(expected, System.Globalization.CultureInfo.InvariantCulture));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("soon")]
    [InlineData("31/31/2026")]
    public void RejectsWhatIsNotADate(string? text) => DirectiveDate.TryParse(text, out _).Should().BeFalse();

    [Theory]
    [InlineData("UTC", 0)]
    [InlineData("GMT", 0)]
    [InlineData("UT", 0)]
    [InlineData("Z", 0)]
    [InlineData("EST", -5)]
    [InlineData("EDT", -4)]
    [InlineData("CST", -6)]
    [InlineData("CDT", -5)]
    [InlineData("MST", -7)]
    [InlineData("MDT", -6)]
    [InlineData("PST", -8)]
    [InlineData("PDT", -7)]
    [InlineData("CET", 1)]
    [InlineData("CEST", 2)]
    [InlineData("pst", -8)]
    public void ReadsTheOffsetOfEachZoneName(string zone, int hours)
    {
        DirectiveDate.TryParse($"25 Jun 2010 15:00:00 {zone}", out var value).Should().BeTrue();
        value.Should().Be(new DateTimeOffset(2010, 6, 25, 15, 0, 0, TimeSpan.FromHours(hours)));
        value.Offset.Should().Be(TimeSpan.FromHours(hours));
    }

    [Theory]
    [InlineData("Friday, 25-Jun-10 15:00:00 EST", 15)]
    [InlineData("Fri, 25 Jun 2010 15:00:00 EST", 15)]
    [InlineData("25 Jun 2010 15:00:00 EST", 15)]
    [InlineData("5 Jun 2010 15:00:00 EST", 15)]
    [InlineData("25 Jun 2010 EST", 0)]
    [InlineData("5 Jun 2010 EST", 0)]
    [InlineData("2010-06-25T15:00:00 EST", 15)]
    [InlineData("2010-06-25 15:00:00 EST", 15)]
    [InlineData("2010-06-25 EST", 0)]
    public void ReadsEachFormatWithAZoneName(string text, int hour)
    {
        DirectiveDate.TryParse(text, out var value).Should().BeTrue();
        value.Offset.Should().Be(TimeSpan.FromHours(-5));
        value.Hour.Should().Be(hour);
        value.Month.Should().Be(6);
    }
}
