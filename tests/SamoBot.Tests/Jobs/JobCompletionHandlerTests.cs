using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Core.Domain;
using SamoBot.Core.Jobs;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Jobs;

public class JobCompletionHandlerTests
{
    [Fact]
    public async Task HandleBatch_PublishesOneEventForEachCompletedJobAndReturnsZero()
    {
        var jobs = new FakeCrawlJobRepository { FinishedJobIds = [3, 4] };
        var events = new FakeEventPublisher();

        var count = await new JobCompletionHandler(jobs, events, new FakeTimeProvider(TestData.Now)).HandleBatch(default);

        count.Should().Be(0);
        events.Published.Select(e => (e.JobId, e.EventType)).Should().Equal(
            (3, CrawlEventTypes.JobCompleted),
            (4, CrawlEventTypes.JobCompleted));
    }
}
