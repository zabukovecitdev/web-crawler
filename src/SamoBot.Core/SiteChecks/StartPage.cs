namespace SamoBot.Core.SiteChecks;

public sealed record StartPage(
    Uri Url,
    bool Redirected,
    int StatusCode,
    string? Problem,
    bool BlockedByRobots,
    byte[]? Content,
    string? ContentType)
{
    public bool Reachable => Problem is null && !BlockedByRobots && StatusCode is >= 200 and <= 299;

    public static StartPage Failed(Uri url, bool redirected, string problem) =>
        new(url, redirected, 0, problem, false, null, null);

    public static StartPage Blocked(Uri url, bool redirected) =>
        new(url, redirected, 0, null, true, null, null);
}
