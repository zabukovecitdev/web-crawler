namespace SamoBot.Core.Common;

public interface IWorkHandler
{
    Task<int> HandleBatch(CancellationToken ct);
}
