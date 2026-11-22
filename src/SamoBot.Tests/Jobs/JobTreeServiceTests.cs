using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Core.Jobs;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Jobs;

public class JobTreeServiceTests
{
    private static JobTreeService Service(FakeCrawlJobRepository jobs, params FolderGroup[] groups) => new(jobs, new FakeJobTreeReader(groups));

    private static FakeCrawlJobRepository WithJob()
    {
        var jobs = new FakeCrawlJobRepository();
        jobs.Add(new CrawlJob { Id = 5, SeedUrls = ["https://a.example/"], AllowedHosts = ["a.example"] });
        return jobs;
    }

    [Fact]
    public async Task TheTreeCarriesTheProgressOfEachFolder()
    {
        var group = new FolderGroup("blog/", "2026/", new FolderCounts(64, 40, 2, 0, 0, 1, 21, null, null));

        var tree = (await Service(WithJob(), group).Tree(5, "/", 2, default)).Value;

        tree.Host.Should().Be("a.example");
        tree.Total.Should().Be(group.Counts);
        tree.Nodes.Single().Children!.Single().Counts.Done.Should().Be(40);
    }

    [Fact]
    public async Task AnUnknownJobIsNotFound() =>
        (await Service(new FakeCrawlJobRepository()).Tree(1, "/", 1, default)).Errors.Single().Should().BeOfType<JobNotFoundError>();

    [Fact]
    public async Task ABadParentIsRefused() =>
        (await Service(WithJob()).Tree(5, "blog", 1, default)).Errors.Single().Should().BeOfType<InvalidRequestError>();
}
