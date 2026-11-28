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
            Policy = """{"directives":["noindex (meta robots)"]}"""
        };

        var parsed = ParsedPageMapper.Map(row, new LinkCounts(4, 3));

        parsed.WordCount.Should().Be(3);
        parsed.Headings.Should().Equal(new PageHeading(1, "H"));
        parsed.ImageCount.Should().Be(2);
        parsed.JsonLdBlocks.Should().Be(1);
        parsed.OpenGraph.Should().ContainKey("og:title");
        parsed.RobotsDirectives.Should().Be("noindex (meta robots)");
        ParsedPageMapper.Map(new PageRow { Headings = "not json", Policy = "{}" }, new LinkCounts(0, 0)).Headings.Should().BeEmpty();
    }

    [Fact]
    public void TheDirectivesOfTheStoredPolicyAreShownWithTheirSource()
    {
        var row = new PageRow { Policy = """{"index":false,"directives":["noindex (meta robots)","nofollow (X-Robots-Tag)"]}""" };

        ParsedPageMapper.Map(row, new LinkCounts(0, 0)).RobotsDirectives.Should().Be("noindex (meta robots), nofollow (X-Robots-Tag)");
    }

    [Fact]
    public void AParsedDocumentWithoutAPolicyIsAnError()
    {
        var map = () => ParsedPageMapper.Map(new PageRow { Id = 7 }, new LinkCounts(0, 0));

        map.Should().Throw<InvalidOperationException>().WithMessage("*page 7*");
    }
}
