using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeBrowserPauseStore : IBrowserPauseStore
{
    public IReadOnlyList<int> Paused { get; set; } = [];
    public IReadOnlyList<int> Resumed { get; set; } = [];
    public int PauseCalls { get; private set; }
    public int ResumeCalls { get; private set; }

    public Task<IReadOnlyList<int>> PauseJobsNeedingBrowser(DateTimeOffset now, CancellationToken ct)
    {
        PauseCalls++;
        return Task.FromResult(Paused);
    }

    public Task<IReadOnlyList<int>> ResumeJobsPausedForBrowser(DateTimeOffset now, CancellationToken ct)
    {
        ResumeCalls++;
        return Task.FromResult(Resumed);
    }
}

public sealed class FakeReviewPauseStore(IReadOnlyList<int> paused) : IReviewPauseStore
{
    public Task<IReadOnlyList<int>> PauseJobsAtTheReviewPoint(DateTimeOffset now, CancellationToken ct) => Task.FromResult(paused);
}
