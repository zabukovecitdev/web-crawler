namespace SamoBot.Core.Domain;

public sealed record BrowserProbeResult(bool Available, string? Reason)
{
    public static BrowserProbeResult Up() => new(true, null);

    public static BrowserProbeResult Down(string reason) => new(false, reason);
}

public sealed record BrowserStatus(bool Available, string? Reason, DateTimeOffset CheckedAt);
