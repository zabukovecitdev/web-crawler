namespace SamoBot.Adapters.Browser;

public sealed class ChromeRenderingOptions
{
    public const string SectionName = "ChromeRendering";

    /// <summary>When set, Playwright connects over CDP (for example http://chrome:9222).</summary>
    public string? CdpEndpoint { get; set; }

    /// <summary>The navigation timeout of a page, and the default of every other browser call. Default 30000.</summary>
    public int DefaultTimeoutMs { get; set; } = 30_000;

    /// <summary>One cap around the whole render (connect, navigation, reading the page). Default 45000.</summary>
    public int TotalTimeoutMs { get; set; } = 45_000;

    public bool Enabled { get; set; }

    /// <summary>The one rule that decides if the browser fetcher is in use. The HTTP fetcher is the fallback.</summary>
    public bool IsAvailable => Enabled && !string.IsNullOrWhiteSpace(CdpEndpoint);
}
