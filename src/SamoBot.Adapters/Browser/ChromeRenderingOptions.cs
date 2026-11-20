namespace SamoBot.Adapters.Browser;

public sealed class ChromeRenderingOptions
{
    public const string SectionName = "ChromeRendering";

    public string? CdpEndpoint { get; set; }

    public int DefaultTimeoutMs { get; set; } = 30_000;

    public int TotalTimeoutMs { get; set; } = 45_000;

    public bool Enabled { get; set; }

    public bool IsAvailable => Enabled && !string.IsNullOrWhiteSpace(CdpEndpoint);
}
