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

    /// <summary>
    /// How long resolved addresses of a host are reused before DNS is asked again. Default 60. 0 turns the cache off.
    /// Cached addresses are checked against the address policy on every use.
    /// </summary>
    public int DnsCacheMinutes { get; set; } = 60;

    /// <summary>DNS resolution and connect together. Default 5. 0 turns it off.</summary>
    public int ConnectTimeoutSeconds { get; set; } = 5;

    /// <summary>From sending the request until the response headers arrive. Default 10. 0 turns it off.</summary>
    public int FirstByteTimeoutSeconds { get; set; } = 10;

    /// <summary>One deadline for a whole page request, the body read included. Default 20. 0 turns it off.</summary>
    public int RequestTimeoutSeconds { get; set; } = 20;

    /// <summary>One deadline for a robots.txt request (each of the https and http attempts), body included. Default 10.</summary>
    public int RobotsTimeoutSeconds { get; set; } = 10;

    /// <summary>One deadline for each sitemap file, body included. Default 30.</summary>
    public int SitemapTimeoutSeconds { get; set; } = 30;
}
