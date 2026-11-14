namespace SamoBot.Adapters.Http;

/// <summary>Network safety settings of the crawler (configuration section <c>Crawler</c>).</summary>
public sealed class CrawlerNetworkOptions
{
    public const string SectionName = "Crawler";

    /// <summary>
    /// Development and tests only. When true, the address policy allows every address (loopback, private, metadata).
    /// The worker and the API log a warning at startup when it is on.
    /// </summary>
    public bool AllowPrivateAddresses { get; set; }

    /// <summary>
    /// Use the system or environment HTTP proxy for fetching. Default false. A proxy would make the connect check
    /// see the proxy address instead of the target address, so the check would no longer protect the target.
    /// </summary>
    public bool UseProxy { get; set; }
}
