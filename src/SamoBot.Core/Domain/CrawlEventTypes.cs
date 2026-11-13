namespace SamoBot.Core.Domain;

/// <summary>The event types that the crawler publishes. The dashboard reads these names.</summary>
public static class CrawlEventTypes
{
    public const string JobCreated = "JobCreated";
    public const string JobStarted = "JobStarted";
    public const string JobPaused = "JobPaused";
    public const string JobCancelled = "JobCancelled";
    public const string JobCompleted = "JobCompleted";
    public const string FetchCompleted = "FetchCompleted";
}
