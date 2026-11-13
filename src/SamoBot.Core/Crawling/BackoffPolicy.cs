namespace SamoBot.Core.Crawling;

public static class BackoffPolicy
{
    private static readonly TimeSpan BaseDelay = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan MaxDelay = TimeSpan.FromHours(1);
    private const int MaxExponent = 10;

    /// <summary>Returns 1 minute * 2^failCount, with a maximum of 1 hour.</summary>
    public static TimeSpan DelayFor(int failCount)
    {
        var exponent = Math.Clamp(failCount, 0, MaxExponent);
        var delay = BaseDelay * Math.Pow(2, exponent);
        return delay > MaxDelay ? MaxDelay : delay;
    }
}
