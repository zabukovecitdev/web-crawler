namespace SamoBot.Core.Common;

public interface IWorkHandler
{
    /// <summary>Processes one batch. Returns the number of processed items.</summary>
    Task<int> HandleBatch(CancellationToken ct);
}
