using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeBrowserProbe(bool available = true) : IBrowserProbe
{
    public bool Available { get; set; } = available;
    public int Probes { get; private set; }

    public Task<BrowserProbeResult> Probe(CancellationToken ct)
    {
        Probes++;
        return Task.FromResult(Available ? BrowserProbeResult.Up() : BrowserProbeResult.Down("The browser service did not answer."));
    }
}
