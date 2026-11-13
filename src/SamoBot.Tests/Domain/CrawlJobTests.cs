using FluentAssertions;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Domain;

public class CrawlJobTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    private static CrawlJob JobIn(CrawlJobStatus status)
    {
        var job = new CrawlJob { Id = 1 };
        switch (status)
        {
            case CrawlJobStatus.Running:
                job.Start(Now);
                break;
            case CrawlJobStatus.Paused:
                job.Start(Now);
                job.Pause(Now);
                break;
            case CrawlJobStatus.Completed:
                job.Complete(Now);
                break;
            case CrawlJobStatus.Cancelled:
                job.Cancel(Now);
                break;
        }

        return job;
    }

    [Theory]
    [InlineData(CrawlJobStatus.Pending, true)]
    [InlineData(CrawlJobStatus.Paused, true)]
    [InlineData(CrawlJobStatus.Running, false)]
    [InlineData(CrawlJobStatus.Completed, false)]
    [InlineData(CrawlJobStatus.Cancelled, false)]
    public void StartIsAllowedOnlyFromPendingOrPaused(CrawlJobStatus from, bool allowed)
    {
        var job = JobIn(from);

        job.Start(Now).IsSuccess.Should().Be(allowed);
        job.Status.Should().Be(allowed ? CrawlJobStatus.Running : from);
    }

    [Theory]
    [InlineData(CrawlJobStatus.Running, true)]
    [InlineData(CrawlJobStatus.Pending, false)]
    [InlineData(CrawlJobStatus.Paused, false)]
    [InlineData(CrawlJobStatus.Completed, false)]
    public void PauseIsAllowedOnlyFromRunning(CrawlJobStatus from, bool allowed)
    {
        var job = JobIn(from);

        job.Pause(Now).IsSuccess.Should().Be(allowed);
        job.Status.Should().Be(allowed ? CrawlJobStatus.Paused : from);
    }

    [Theory]
    [InlineData(CrawlJobStatus.Pending, true)]
    [InlineData(CrawlJobStatus.Running, true)]
    [InlineData(CrawlJobStatus.Paused, true)]
    [InlineData(CrawlJobStatus.Cancelled, true)]
    [InlineData(CrawlJobStatus.Completed, false)]
    public void CancelIsAllowedFromEveryStatusExceptCompleted(CrawlJobStatus from, bool allowed)
    {
        var job = JobIn(from);

        job.Cancel(Now).IsSuccess.Should().Be(allowed);
        job.Status.Should().Be(allowed ? CrawlJobStatus.Cancelled : from);
    }

    [Fact]
    public void StartSetsStartedAtOnlyOnce()
    {
        var job = JobIn(CrawlJobStatus.Paused);

        job.Start(Now.AddHours(1));

        job.StartedAt.Should().Be(Now);
        job.UpdatedAt.Should().Be(Now.AddHours(1));
    }

    [Fact]
    public void CompleteSetsStatusAndCompletedAt()
    {
        var job = JobIn(CrawlJobStatus.Running);

        job.Complete(Now.AddHours(2));

        job.Status.Should().Be(CrawlJobStatus.Completed);
        job.CompletedAt.Should().Be(Now.AddHours(2));
    }
}
