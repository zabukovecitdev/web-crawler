namespace SamoBot.Core.Crawling;

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
