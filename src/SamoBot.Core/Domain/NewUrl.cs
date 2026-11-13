namespace SamoBot.Core.Domain;

/// <summary>
/// A URL that is not in the frontier yet. The job flags are copied from the job on insert.
/// The host is the URI authority: the host name plus a non-default port.
/// </summary>
public sealed record NewUrl(int CrawlJobId, string Url, string NormalizedUrl, string Host, int Depth);
