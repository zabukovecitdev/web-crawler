using System.Text;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Http;

/// <summary>
/// Downloads robots.txt. The host key can include a non-default port, so the source tries https first and then http.
/// A found file gives its rules. A missing file (4xx, or a redirect that never ends) allows everything.
/// A server error or a network error blocks the host for a short time.
/// </summary>
public sealed class HttpRobotsSource(HttpClient httpClient, TimeProvider timeProvider) : IRobotsSource
{
    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
    private const int MaxBodyBytes = 512 * 1024;
    private static readonly TimeSpan FoundTtl = TimeSpan.FromHours(24);
    private static readonly TimeSpan ErrorTtl = TimeSpan.FromHours(1);
    private static readonly string[] Schemes = ["https", "http"];

    public async Task<RobotsRules> Download(string host, CancellationToken ct)
    {
        var now = timeProvider.GetUtcNow();
        foreach (var scheme in Schemes)
        {
            var rules = await TryDownload(scheme, host, now, ct);
            if (rules is not null)
            {
                return rules;
            }
        }

        return RobotsRules.DisallowAll(host, now, ErrorTtl);
    }

    private async Task<RobotsRules?> TryDownload(string scheme, string host, DateTimeOffset now, CancellationToken ct)
    {
        try
        {
            using var response = await httpClient.GetAsync($"{scheme}://{host}/robots.txt", HttpCompletionOption.ResponseHeadersRead, ct);
            return await ToRules(host, response, now, ct);
        }
        catch (HttpRequestException)
        {
            return null;
        }
        catch (IOException)
        {
            return null;
        }
        catch (TaskCanceledException) when (!ct.IsCancellationRequested)
        {
            return null;
        }
    }

    private static async Task<RobotsRules> ToRules(string host, HttpResponseMessage response, DateTimeOffset now, CancellationToken ct) =>
        (int)response.StatusCode switch
        {
            >= 200 and <= 299 => new RobotsRules(host, await ReadBody(response, ct), now, now + FoundTtl),
            >= 300 and <= 499 => RobotsRules.AllowAll(host, now, FoundTtl),
            _ => RobotsRules.DisallowAll(host, now, ErrorTtl)
        };

    private static async Task<string> ReadBody(HttpResponseMessage response, CancellationToken ct)
    {
        await using var body = await response.Content.ReadAsStreamAsync(ct);
        return Encoding.UTF8.GetString(await BodyReader.ReadLimited(body, MaxBodyBytes, ct));
    }
}
