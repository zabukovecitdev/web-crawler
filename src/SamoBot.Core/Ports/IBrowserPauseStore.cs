namespace SamoBot.Core.Ports;

public interface IBrowserPauseStore
{
    Task<IReadOnlyList<int>> PauseJobsNeedingBrowser(DateTimeOffset now, CancellationToken ct);

    Task<IReadOnlyList<int>> ResumeJobsPausedForBrowser(DateTimeOffset now, CancellationToken ct);
}
