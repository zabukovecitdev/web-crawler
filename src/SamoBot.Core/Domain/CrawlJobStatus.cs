namespace SamoBot.Core.Domain;

public enum CrawlJobStatus
{
    Draft,
    Pending,
    Running,
    Paused,
    Completed,
    Cancelled,
    Failed
}
