using FluentAssertions;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Core.Jobs;
using SamoBot.Core.SiteChecks;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.SiteChecks;

public class PlanTreeServiceTests
{
    private static PlanTreeService Service(FakeSitemapPlanReader reader)
    {
        var rules = new RobotsRules("a.example", "User-agent: *\nDisallow: /admin/\n", TestData.Now, TestData.Now.AddDays(1));
        var gate = new RobotsGate(new MemoryCache(Options.Create(new MemoryCacheOptions())), new FakeRobotsStore(), new FakeRobotsSource(rules), new FakeTimeProvider(TestData.Now));
        return new PlanTreeService(reader, gate);
    }

    [Fact]
    public async Task TheTreeHoldsTheFoldersOfTheSitemapWithTheBlockedOnesMarked()
    {
        var groups = new[]
        {
            new FolderGroup("guide/", "install", FolderCounts.None with { Pages = 1 }),
            new FolderGroup("admin/", "x", FolderCounts.None with { Pages = 50 })
        };
        var reader = new FakeSitemapPlanReader(new SitemapPlanStatus(1, 51, null, null, 0), groups);

        var tree = (await Service(reader).Tree("a.example", "/", 2, default)).Value;

        tree.Total.Pages.Should().Be(51);
        tree.Nodes.Select(node => (node.Prefix, node.Blocked, node.Counts.Pages)).Should().Equal(("/admin/", true, 50), ("/guide/", false, 1));
    }

    [Theory]
    [InlineData("blog/")]
    [InlineData("/blog")]
    [InlineData("/a?b/")]
    public async Task AParentThatIsNotAFolderPathIsRefusedBeforeTheDatabaseIsAsked(string parent)
    {
        var reader = new FakeSitemapPlanReader(new SitemapPlanStatus(0, 0, null, null, 0));

        var result = await Service(reader).Tree("a.example", parent, 2, default);

        result.Errors.Should().NotBeEmpty().And.AllBeOfType<InvalidRequestError>();
        reader.LastParent.Should().BeNull();
    }
}
