namespace Samobot.Infrastructure.Enums;

public static class IndexJobStatusExtensions
{
    public static string AsString(this IndexJobStatus status) => status switch
    {
        IndexJobStatus.Pending => "Pending",
        IndexJobStatus.InProgress => "InProgress",
        IndexJobStatus.Completed => "Completed",
        IndexJobStatus.Failed => "Failed",
        _ => throw new ArgumentOutOfRangeException(nameof(status), status, null)
    };
}
