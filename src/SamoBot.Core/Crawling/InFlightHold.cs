namespace SamoBot.Core.Crawling;

/// <summary>
/// How long a claim books its host while one request is in flight: the longest possible fetch (the larger of the
/// request deadline and the browser total deadline) plus a margin. A claim puts <c>NextAllowedAt</c> this far ahead,
/// and completing the URL releases it. If a worker dies, the hold simply expires. A deadline of zero or less means
/// the fetch has no deadline, so the hold is the cap, which is the URL lease.
/// </summary>
public sealed record InFlightHold(TimeSpan Duration)
{
    public static InFlightHold Calculate(TimeSpan requestTimeout, TimeSpan browserTotalTimeout, TimeSpan margin, TimeSpan cap)
    {
        if (requestTimeout <= TimeSpan.Zero || browserTotalTimeout <= TimeSpan.Zero)
        {
            return new InFlightHold(cap);
        }

        var longest = requestTimeout >= browserTotalTimeout ? requestTimeout : browserTotalTimeout;
        var hold = longest + (margin > TimeSpan.Zero ? margin : TimeSpan.Zero);
        return new InFlightHold(hold <= cap ? hold : cap);
    }
}
