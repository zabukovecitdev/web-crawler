using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Core.Parsing;

namespace SamoBot.Tests.Parsing;

public class JobScopeTests
{
    private static FetchToParse Parent(int depth = 0, int? maxDepth = null) => new()
    {
        FetchId = 1,
        UrlId = 2,
        CrawlJobId = 7,
        Url = "https://a.example/",
        Depth = depth,
        ObjectName = "o",
        AllowedHosts = ["a.example"],
        MaxDepth = maxDepth
    };

    [Fact]
    public void FilterKeepsOnlyAllowedHostsAndSetsChildDepth()
    {
        var result = JobScope.Filter(Parent(depth: 1), ["https://a.example/p", "https://other.example/q"]);

        result.Should().Equal(new NewUrl(7, "https://a.example/p", "https://a.example/p", "a.example", 2));
    }

    [Fact]
    public void FilterComparesHostsWithoutCase()
    {
        var parent = Parent() with { AllowedHosts = ["A.Example"] };

        JobScope.Filter(parent, ["https://a.example/p"]).Should().HaveCount(1);
    }

    [Fact]
    public void FilterWhenChildDepthExceedsMaxDepthReturnsNothing()
    {
        JobScope.Filter(Parent(depth: 2, maxDepth: 2), ["https://a.example/p"]).Should().BeEmpty();
    }

    [Fact]
    public void FilterWhenChildDepthEqualsMaxDepthKeepsLinks()
    {
        JobScope.Filter(Parent(depth: 1, maxDepth: 2), ["https://a.example/p"]).Should().HaveCount(1);
    }

    [Fact]
    public void FilterTreatsAHostWithAnotherPortAsAnotherHost()
    {
        var parent = Parent() with { AllowedHosts = ["a.example:8080"] };

        var result = JobScope.Filter(parent, ["https://a.example:8080/p", "https://a.example/q"]);

        result.Should().Equal(new NewUrl(7, "https://a.example:8080/p", "https://a.example:8080/p", "a.example:8080", 1));
    }
}
