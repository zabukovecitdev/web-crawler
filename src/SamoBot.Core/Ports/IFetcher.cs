using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

/// <summary>Downloads one URL. An implementation never returns null and never throws for an HTTP error status.</summary>
public interface IFetcher
{
    bool Supports(ClaimedUrl url);

    Task<FetchResponse> Fetch(ClaimedUrl url, CancellationToken ct);
}
