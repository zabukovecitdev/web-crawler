using Samobot.Infrastructure.Enums;

namespace SamoBot.Infrastructure.Data;

public class IndexJobEntity
{
    public int Id { get; set; }
    public int DocumentId { get; set; }
    public string Operation { get; set; } = nameof(IndexJobOperation.Index);
    public string Status { get; set; } = nameof(IndexJobStatus.Pending);
    public int Attempts { get; set; }
    public string? LastError { get; set; }
    public DateTimeOffset ScheduledAt { get; set; }
    public DateTimeOffset? ProcessedAt { get; set; }
    public DateTimeOffset CreatedAt { get; set; }

    public IndexJobOperation GetOperation() =>
        Enum.TryParse<IndexJobOperation>(Operation, out var op) ? op : IndexJobOperation.Index;

    public IndexJobStatus GetStatus() =>
        Enum.TryParse<IndexJobStatus>(Status, out var status) ? status : IndexJobStatus.Pending;
}
