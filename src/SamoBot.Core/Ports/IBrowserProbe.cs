using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IBrowserProbe
{
    Task<BrowserProbeResult> Probe(CancellationToken ct);
}
