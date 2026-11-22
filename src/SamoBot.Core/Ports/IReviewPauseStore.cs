namespace SamoBot.Core.Ports;

public interface IReviewPauseStore
{
    Task<IReadOnlyList<int>> PauseJobsAtTheReviewPoint(DateTimeOffset now, CancellationToken ct);
}
