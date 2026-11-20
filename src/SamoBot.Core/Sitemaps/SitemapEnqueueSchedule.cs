namespace SamoBot.Core.Sitemaps;

public sealed class SitemapEnqueueSchedule(TimeProvider timeProvider)
{
    public static readonly TimeSpan Interval = TimeSpan.FromMinutes(5);

    private long _lastTicks = long.MinValue;

    public bool TryStart()
    {
        var now = timeProvider.GetUtcNow().UtcTicks;
        var last = Interlocked.Read(ref _lastTicks);
        if (last != long.MinValue && now - last < Interval.Ticks)
        {
            return false;
        }

        return Interlocked.CompareExchange(ref _lastTicks, now, last) == last;
    }
}
