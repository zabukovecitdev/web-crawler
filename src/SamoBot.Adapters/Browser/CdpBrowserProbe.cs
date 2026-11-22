using Microsoft.Extensions.Options;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Browser;

public sealed class CdpBrowserProbe(HttpClient client, IOptions<ChromeRenderingOptions> options) : IBrowserProbe
{
    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(3);

    public async Task<BrowserProbeResult> Probe(CancellationToken ct)
    {
        if (!options.Value.IsAvailable)
        {
            return BrowserProbeResult.Down("Browser rendering is not turned on for this server.");
        }

        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(ct);
        deadline.CancelAfter(Timeout);
        try
        {
            using var response = await client.GetAsync($"{options.Value.CdpEndpoint!.TrimEnd('/')}/json/version", deadline.Token);
            return response.IsSuccessStatusCode
                ? BrowserProbeResult.Up()
                : BrowserProbeResult.Down($"The browser service answered with status {(int)response.StatusCode}.");
        }
        catch (Exception ex) when (ex is HttpRequestException or OperationCanceledException && !ct.IsCancellationRequested)
        {
            return BrowserProbeResult.Down("The browser service did not answer.");
        }
    }
}
