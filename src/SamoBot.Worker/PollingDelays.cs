namespace SamoBot.Worker;

public static class PollingDelays
{
    public static readonly TimeSpan Idle = TimeSpan.FromSeconds(1);

    public static readonly TimeSpan Error = TimeSpan.FromSeconds(5);
}
