using System.Text;
using Microsoft.Extensions.Options;
using SamoBot.Core.Common;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Http;

public sealed class HttpRobotsSource(
    HttpClient httpClient,
    IOptions<CrawlerNetworkOptions> network,
    TimeProvider timeProvider) : IRobotsSource
{
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
        using var deadline = new RequestDeadline(
            RequestDeadline.Seconds(network.Value.RobotsTimeoutSeconds),
            RequestDeadline.Seconds(network.Value.FirstByteTimeoutSeconds),
            timeProvider,
            ct);
        try
        {
            using var response = await httpClient.GetAsync($"{scheme}://{host}/robots.txt", HttpCompletionOption.ResponseHeadersRead, deadline.Token);
            deadline.HeadersReceived();
            return await ToRules(host, response, now, deadline.Token);
        }
        catch (HttpRequestException ex) when (BlockedAddressException.IsCause(ex))
        {
            throw new BlockedAddressException(host);
        }
        catch (HttpRequestException)
        {
            return null;
        }
        catch (IOException)
        {
            return null;
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
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
        return Encoding.UTF8.GetString((await BodyReader.ReadLimited(body, MaxBodyBytes, ct)).Bytes);
    }
}
