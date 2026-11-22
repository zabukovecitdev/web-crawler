using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Browser;

public sealed class BrowserAvailability(IBrowserProbe probe, TimeProvider timeProvider)
{
    public static readonly TimeSpan MaxAge = TimeSpan.FromSeconds(5);

    private BrowserStatus? _last;

    public async Task<BrowserStatus> Current(CancellationToken ct)
    {
        var last = _last;
        if (last is not null && timeProvider.GetUtcNow() - last.CheckedAt < MaxAge)
        {
            return last;
        }

        return await Refresh(ct);
    }

    public async Task<BrowserStatus> Refresh(CancellationToken ct)
    {
        var result = await probe.Probe(ct);
        var status = new BrowserStatus(result.Available, result.Reason, timeProvider.GetUtcNow());
        _last = status;
        return status;
    }
}
