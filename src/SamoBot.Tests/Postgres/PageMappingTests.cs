using FluentAssertions;
using SamoBot.Adapters.Postgres;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Postgres;

public class PageMappingTests
{
    [Fact]
    public void LikePatternEscapesWildcardsAndDropsBlankText()
    {
        LikePattern.Contains(" 50%_off\\ ").Should().Be("%50\\%\\_off\\\\%");
        LikePattern.Contains("   ").Should().BeNull();
        LikePattern.Contains(null).Should().BeNull();
    }

    [Fact]
    public void ARowMapsToASummaryWithPathAndDerivedState()
    {
        var row = new PageRow { Id = 3, Url = "https://a.example/x/y?q=1", StatusCode = 301 };

        var summary = row.ToSummary();

        summary.Path.Should().Be("/x/y?q=1");
        summary.ContentState.Should().Be(ContentState.Redirect);
    }

    [Fact]
    public void ParsedColumnsMapToCountsAndTolerateBrokenJson()
    {
        var row = new PageRow
        {
            Title = "T",
            BodyText = "one two  three",
            Headings = """[{"level":1,"text":"H"}]""",
            Images = """[{"src":"a"},{"src":"b"}]""",
            JsonLdData = "[\"{}\"]",
            OpenGraphData = """{"og:title":"T"}""",
            RobotsDirectives = """{"content":"noindex"}"""
        };

        var parsed = ParsedPageMapper.Map(row);

        parsed.WordCount.Should().Be(3);
        parsed.Headings.Should().Equal(new PageHeading(1, "H"));
        parsed.ImageCount.Should().Be(2);
        parsed.JsonLdBlocks.Should().Be(1);
        parsed.OpenGraph.Should().ContainKey("og:title");
        parsed.RobotsDirectives.Should().Be("noindex");
        ParsedPageMapper.Map(new PageRow { Headings = "not json" }).Headings.Should().BeEmpty();
    }
}
