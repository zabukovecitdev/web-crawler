using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IFetcher
{
    bool Supports(ClaimedUrl url);

    Task<FetchResponse> Fetch(ClaimedUrl url, CancellationToken ct);
}
