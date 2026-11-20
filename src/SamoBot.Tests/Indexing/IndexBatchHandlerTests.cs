using FluentAssertions;
using SamoBot.Core.Directives;
using SamoBot.Core.Domain;
using SamoBot.Core.Indexing;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Indexing;

public class IndexBatchHandlerTests
{
    [Fact]
    public async Task HandleBatchIndexesAllPagesInOneCallThenMarksThemIndexed()
    {
        var queue = new FakeIndexBacklog(
            new PageToIndex { PageId = 10, UrlFetchId = 1, DiscoveredUrlId = 1, Policy = PagePolicy.Default },
            new PageToIndex { PageId = 11, UrlFetchId = 2, DiscoveredUrlId = 2, Policy = PagePolicy.Default });
        var index = new FakeSearchIndex();

        var count = await new IndexBatchHandler(queue, index, TimeProvider.System).HandleBatch(default);

        count.Should().Be(2);
        queue.LastBatchSize.Should().Be(50);
        index.Calls.Should().ContainSingle().Which.Should().HaveCount(2);
        queue.Indexed.Should().Equal(10, 11);
    }

    [Fact]
    public async Task HandleBatchWithoutPagesDoesNotCallTheIndex()
    {
        var index = new FakeSearchIndex();

        var count = await new IndexBatchHandler(new FakeIndexBacklog(), index, TimeProvider.System).HandleBatch(default);

        count.Should().Be(0);
        index.Calls.Should().BeEmpty();
    }

    [Fact]
    public async Task PagesThePolicyKeepsOutAreRemovedFromTheIndexAndStillMarked()
    {
        var time = new Microsoft.Extensions.Time.Testing.FakeTimeProvider(TestData.Now);
        var queue = new FakeIndexBacklog(
            new PageToIndex { PageId = 10, UrlFetchId = 1, DiscoveredUrlId = 1, Policy = PagePolicy.Default },
            new PageToIndex { PageId = 11, UrlFetchId = 2, DiscoveredUrlId = 2, Policy = PagePolicy.Default with { Index = false } },
            new PageToIndex { PageId = 12, UrlFetchId = 3, DiscoveredUrlId = 3, Policy = PagePolicy.Default with { IndexUntil = TestData.Now } },
            new PageToIndex { PageId = 13, UrlFetchId = 4, DiscoveredUrlId = 4, Policy = PagePolicy.Default with { IndexUntil = TestData.Now.AddDays(1) } });
        var index = new FakeSearchIndex();

        await new IndexBatchHandler(queue, index, time).HandleBatch(default);

        index.Calls.Single().Select(p => p.PageId).Should().Equal(10, 13);
        index.Removed.Single().Should().Equal(2, 3);
        queue.Indexed.Should().Equal(10, 11, 12, 13);
    }

    [Fact]
    public async Task TheNewestPageOfAUrlDecides()
    {
        var queue = new FakeIndexBacklog(
            new PageToIndex { PageId = 10, UrlFetchId = 1, DiscoveredUrlId = 1, Policy = PagePolicy.Default },
            new PageToIndex { PageId = 11, UrlFetchId = 2, DiscoveredUrlId = 1, Policy = PagePolicy.Default with { Index = false } });
        var index = new FakeSearchIndex();

        await new IndexBatchHandler(queue, index, TimeProvider.System).HandleBatch(default);

        index.Calls.Single().Should().BeEmpty();
        index.Removed.Single().Should().Equal(1);
    }
}
