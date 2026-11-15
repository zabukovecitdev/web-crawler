using FluentAssertions;
using SamoBot.Core.Directives;

namespace SamoBot.Tests.Directives;

public class RobotsDirectiveParserTests
{
    private static IEnumerable<string> Names(IEnumerable<RobotsDirective> directives) => directives.Select(d => d.Name);

    [Fact]
    public void AListIsSplitOnCommasTrimmedAndLowerCased() =>
        Names(RobotsDirectiveParser.ParseList(" NoIndex ,nofollow,, NOARCHIVE ", "meta robots")).Should().Equal("noindex", "nofollow", "noarchive");

    [Fact]
    public void ADirectiveWithAValueKeepsTheValue()
    {
        var directives = RobotsDirectiveParser.ParseList("max-snippet: 20, unavailable_after: 2026-12-31", "meta robots");

        directives.Should().Equal(
            new RobotsDirective("max-snippet", "20", "meta robots"),
            new RobotsDirective("unavailable_after", "2026-12-31", "meta robots"));
    }

    [Fact]
    public void ADateWithACommaStaysOneValue() =>
        RobotsDirectiveParser.ParseList("unavailable_after: Friday, 25-Jun-10 15:00:00 PST, noindex", "h")
            .Should().Equal(new RobotsDirective("unavailable_after", "Friday, 25-Jun-10 15:00:00 PST", "h"), new RobotsDirective("noindex", null, "h"));

    [Theory]
    [InlineData("nofollow", "nofollow")]
    [InlineData("samobot: nofollow", "nofollow")]
    [InlineData("SamoBot: noindex, nofollow", "noindex,nofollow")]
    [InlineData("googlebot: noindex, samobot: nofollow", "nofollow")]
    [InlineData("googlebot: nofollow", "")]
    [InlineData("googlebot: noindex, nofollow", "")]
    [InlineData("max-snippet: 20, nofollow", "max-snippet,nofollow")]
    [InlineData("unavailable_after: 25 Jun 2010 15:00:00 PST", "unavailable_after")]
    public void AScopedValueKeepsOnlyTheDirectivesForThisCrawler(string line, string expected) =>
        string.Join(',', Names(RobotsDirectiveParser.ParseScoped(line, "SamoBot", "X-Robots-Tag"))).Should().Be(expected);

    [Fact]
    public void NothingGivesNothing()
    {
        RobotsDirectiveParser.ParseList(null, "m").Should().BeEmpty();
        RobotsDirectiveParser.ParseScoped("  ", "SamoBot", "h").Should().BeEmpty();
    }
}
