namespace SamoBot.Core.Sitemaps;

/// <summary>Lets the sitemap loop queue finished hosts at most once for each <see cref="Interval"/> (the loop itself runs every second).</summary>
public sealed class SitemapEnqueueSchedule(TimeProvider timeProvider)
{
    public static readonly TimeSpan Interval = TimeSpan.FromMinutes(5);

    private long _lastTicks = long.MinValue;

    /// <summary>True for the first call and again once <see cref="Interval"/> has passed since the last true.</summary>
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
