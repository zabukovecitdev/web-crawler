using FluentAssertions;
using SamoBot.Core.Directives.Sources;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Directives;

public class DirectiveSourceTests
{
    [Fact]
    public void TheMetaSourceReadsRobotsAndThisCrawlerInAnyCaseAndNothingElse()
    {
        var page = new ParsedPage
        {
            MetaTags =
            [
                new MetaTag("ROBOTS", "noindex"),
                new MetaTag("samobot", "noarchive"),
                new MetaTag("googlebot", "nofollow"),
                new MetaTag("robots", "nosnippet", HttpEquiv: true)
            ]
        };

        var directives = new MetaRobotsTagSource().Read(TestDirectives.Context(page)).ToList();

        directives.Select(d => d.ToString()).Should().Equal("noindex (meta robots)", "noarchive (meta samobot)");
    }

    [Fact]
    public void TheHeaderSourceReadsEveryValueOfTheHeader()
    {
        var headers = TestDirectives.Header("X-Robots-Tag", "noindex", "googlebot: nofollow", "samobot: noarchive");

        var directives = new XRobotsTagSource().Read(TestDirectives.Context(new ParsedPage(), headers: headers)).ToList();

        directives.Select(d => d.Name).Should().Equal("noindex", "noarchive");
        directives.Should().OnlyContain(d => d.Source == "X-Robots-Tag");
    }

    [Fact]
    public void TheHeaderSourceAsksTheFetchersToKeepItsHeader() =>
        new XRobotsTagSource().ResponseHeaders.Should().Equal("X-Robots-Tag");
}
