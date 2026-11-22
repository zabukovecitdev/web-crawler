using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

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
    [InlineData(CrawlJobStatus.Completed, true)]
    [InlineData(CrawlJobStatus.Pending, false)]
    [InlineData(CrawlJobStatus.Running, false)]
    [InlineData(CrawlJobStatus.Paused, false)]
    [InlineData(CrawlJobStatus.Cancelled, false)]
    public void ReprocessIsAllowedOnlyFromCompletedAndClearsTheCompletionTime(CrawlJobStatus from, bool allowed)
    {
        var job = JobIn(from);
        var completedAt = job.CompletedAt;

        job.Reprocess(Now.AddHours(1)).IsSuccess.Should().Be(allowed);
        job.Status.Should().Be(allowed ? CrawlJobStatus.Running : from);
        job.CompletedAt.Should().Be(allowed ? null : completedAt);
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

    [Fact]
    public void ReopenKeepsAPausedJobPaused()
    {
        var job = new CrawlJob { SeedUrls = ["https://a.example/"] };
        job.Start(Now);
        job.Pause(Now);

        job.Reopen(Now, wholeJob: true).IsSuccess.Should().BeTrue();

        job.Status.Should().Be(CrawlJobStatus.Paused);
    }

    [Fact]
    public void ReopenOfAJobThatNeverStartedFails() =>
        new CrawlJob().Reopen(Now, wholeJob: true).IsFailed.Should().BeTrue();

    [Fact]
    public void ReopenOfACancelledJobRunsItAgainOnlyForTheWholeJob()
    {
        var job = new CrawlJob { SeedUrls = ["https://a.example/"] };
        job.Start(Now);
        job.Cancel(Now);

        job.Reopen(Now, wholeJob: false).IsFailed.Should().BeTrue();
        job.Reopen(Now, wholeJob: true).IsSuccess.Should().BeTrue();

        (job.Status, job.CompletedAt).Should().Be((CrawlJobStatus.Running, null));
    }

    [Fact]
    public void CancelSetsCompletedAtOnlyWhenItWorks()
    {
        var cancelled = JobIn(CrawlJobStatus.Running);
        cancelled.Cancel(Now.AddHours(1));
        var completed = JobIn(CrawlJobStatus.Completed);
        var before = completed.CompletedAt;

        completed.Cancel(Now.AddHours(1)).IsFailed.Should().BeTrue();

        cancelled.CompletedAt.Should().Be(Now.AddHours(1));
        completed.CompletedAt.Should().Be(before);
    }

    [Fact]
    public void ARefusedChangeNamesTheStatuses() =>
        JobIn(CrawlJobStatus.Pending).Pause(Now).Errors[0].Message.Should().Be("A job in status Pending cannot change to Paused.");

    [Fact]
    public void CreateWithoutAValidSeedSaysSo() =>
        CrawlJob.Create(TestData.NewJob(["", "not a url"]), Now).Errors[0].Message
            .Should().Be("At least one valid seed URL is required.");

    [Fact]
    public void ReopenSaysWhyItIsRefused()
    {
        new CrawlJob().Reopen(Now, wholeJob: true).Errors[0].Message.Should().Be("The crawl has not started yet.");
        var cancelled = JobIn(CrawlJobStatus.Cancelled);

        cancelled.Reopen(Now, wholeJob: false).Errors[0].Message.Should().Be(
            "This crawl was cancelled. Start a new crawl for this link, or recrawl the whole job.");
    }

    private static CrawlJob NewJob(IReadOnlyList<PathRule>? rules = null, bool fromSitemap = false, bool followLinked = true, int? review = null) =>
        CrawlJob.Create(
            new NewCrawlJob(null, ["https://a.example/guide/install"], null, null, RenderMode.Auto, true, rules ?? [], fromSitemap, followLinked, review),
            Now).Value;

    [Fact]
    public void CreateKeepsTheRenderModeAndPinsTheStartPage()
    {
        var job = NewJob([new PathRule(PathRuleKind.Exclude, "/guide/install", true)]);

        (job.RenderMode, job.JsNeeded, job.NeedsBrowser).Should().Be((RenderMode.Auto, true, true));
        job.PathRules.Should().Equal(new PathRule(PathRuleKind.Include, "/guide/install", true));
    }

    [Fact]
    public void WithoutASitemapLinksAreAlwaysFollowedAndTheReviewIsKept()
    {
        var job = NewJob(fromSitemap: false, followLinked: false, review: 100);

        (job.FollowLinkedPages, job.ReviewAfterPages).Should().Be((true, 100));
    }

    [Fact]
    public void WithASitemapTheLinkedPagesCanBeTurnedOffAndThereIsNoReview()
    {
        var job = NewJob(fromSitemap: true, followLinked: false, review: 100);

        (job.FollowLinkedPages, job.ReviewAfterPages).Should().Be((false, null));
    }

    [Fact]
    public void ChangingTheFoldersOfARunningJobKeepsTheStartPage()
    {
        var job = NewJob();
        job.Start(Now);

        var result = job.ChangePathRules([new PathRule(PathRuleKind.Exclude, "/")], Now);

        result.IsSuccess.Should().BeTrue();
        job.PathRules.Should().Contain(new PathRule(PathRuleKind.Include, "/guide/install", true));
        job.PathRules.Should().Contain(new PathRule(PathRuleKind.Exclude, "/"));
    }

    [Fact]
    public void TheFoldersOfAFinishedJobCannotBeChanged()
    {
        var job = JobIn(CrawlJobStatus.Completed);

        job.ChangePathRules([], Now).Errors[0].Message.Should().Be("The folders of a Completed crawl cannot be changed.");
    }

    [Fact]
    public void PauseForRecordsTheReasonAndStartClearsIt()
    {
        var job = JobIn(CrawlJobStatus.Running);

        job.PauseFor(PauseReasons.BrowserUnavailable, null, Now).IsSuccess.Should().BeTrue();

        job.IsPausedFor(PauseReasons.BrowserUnavailable).Should().BeTrue();
        job.Start(Now);
        (job.Status, job.PauseReason).Should().Be((CrawlJobStatus.Running, null));
    }

    [Fact]
    public void ResumingAfterTheReviewTurnsTheReviewOff()
    {
        var job = NewJob(review: 100);
        job.Start(Now);
        job.PauseFor(PauseReasons.Review, null, Now);

        job.Start(Now);

        job.ReviewAfterPages.Should().BeNull();
    }

    [Fact]
    public void APauseByTheUserHasNoReason()
    {
        var job = JobIn(CrawlJobStatus.Paused);

        (job.Status, job.PauseReason).Should().Be((CrawlJobStatus.Paused, null));
    }

    [Fact]
    public void OnlyARunningJobCanBePausedForAReason() =>
        JobIn(CrawlJobStatus.Pending).PauseFor(PauseReasons.Review, null, Now).IsFailed.Should().BeTrue();
}
