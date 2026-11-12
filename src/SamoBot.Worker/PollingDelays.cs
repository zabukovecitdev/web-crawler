namespace SamoBot.Worker;

public static class PollingDelays
{
    /// <summary>The wait after a batch with no work.</summary>
    public static readonly TimeSpan Idle = TimeSpan.FromSeconds(1);

    /// <summary>The wait after a batch that threw an exception.</summary>
    public static readonly TimeSpan Error = TimeSpan.FromSeconds(5);
}
