namespace SamoBot.Adapters.Browser;

public sealed class ChromeRenderingOptions
{
    public const string SectionName = "ChromeRendering";

    /// <summary>When set, Playwright connects over CDP (for example http://chrome:9222).</summary>
    public string? CdpEndpoint { get; set; }

    public int DefaultTimeoutMs { get; set; } = 60_000;

    public bool Enabled { get; set; }

    /// <summary>The one rule that decides if the browser fetcher is in use. The HTTP fetcher is the fallback.</summary>
    public bool IsAvailable => Enabled && !string.IsNullOrWhiteSpace(CdpEndpoint);
}
