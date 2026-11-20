namespace SamoBot.Core.Domain;

public static class CrawlEventTypes
{
    public const string JobCreated = "JobCreated";
    public const string JobStarted = "JobStarted";
    public const string JobPaused = "JobPaused";
    public const string JobCancelled = "JobCancelled";
    public const string JobCompleted = "JobCompleted";
    public const string JobRecrawlRequested = "JobRecrawlRequested";
    public const string FetchStarted = "FetchStarted";
    public const string FetchCompleted = "FetchCompleted";
}
