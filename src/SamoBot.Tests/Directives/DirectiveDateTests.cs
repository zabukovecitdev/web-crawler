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
}
