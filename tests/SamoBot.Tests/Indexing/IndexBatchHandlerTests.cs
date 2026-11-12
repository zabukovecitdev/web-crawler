using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Core.Indexing;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Indexing;

public class IndexBatchHandlerTests
{
    [Fact]
    public async Task HandleBatch_IndexesAllPagesInOneCallThenMarksThemIndexed()
    {
        var queue = new FakeIndexQueue(
            new PageToIndex { PageId = 10, UrlFetchId = 1 },
            new PageToIndex { PageId = 11, UrlFetchId = 2 });
        var index = new FakeSearchIndex();

        var count = await new IndexBatchHandler(queue, index).HandleBatch(default);

        count.Should().Be(2);
        queue.LastBatchSize.Should().Be(50);
        index.Calls.Should().ContainSingle().Which.Should().HaveCount(2);
        queue.Indexed.Should().Equal(10, 11);
    }

    [Fact]
    public async Task HandleBatch_WithoutPages_DoesNotCallTheIndex()
    {
        var index = new FakeSearchIndex();

        var count = await new IndexBatchHandler(new FakeIndexQueue(), index).HandleBatch(default);

        count.Should().Be(0);
        index.Calls.Should().BeEmpty();
    }
}
