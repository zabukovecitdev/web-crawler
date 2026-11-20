using Microsoft.Extensions.Options;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;
using SamoBot.Core.Sitemaps;

namespace SamoBot.Adapters.Http;

public sealed class HttpSitemapSource(
    HttpClient httpClient,
    IOptions<CrawlerNetworkOptions> network,
    TimeProvider timeProvider) : ISitemapSource
{
    public async Task<SitemapDownload> Download(string url, CancellationToken ct)
    {
        using var deadline = new RequestDeadline(
            RequestDeadline.Seconds(network.Value.SitemapTimeoutSeconds),
            RequestDeadline.Seconds(network.Value.FirstByteTimeoutSeconds),
            timeProvider,
            ct);
        try
        {
            using var response = await httpClient.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, deadline.Token);
            deadline.HeadersReceived();
            return await ToDownload(response, deadline.Token);
        }
        catch (HttpRequestException)
        {
            return Failed;
        }
        catch (IOException)
        {
            return Failed;
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return Failed;
        }
    }

    private static SitemapDownload Failed => new(0, null);

    private static async Task<SitemapDownload> ToDownload(HttpResponseMessage response, CancellationToken ct)
    {
        var status = (int)response.StatusCode;
        if (status is < 200 or > 299 || response.Content.Headers.ContentLength > SitemapLimits.MaxCompressedBytes)
        {
            return new SitemapDownload(status, null);
        }

        await using var body = await response.Content.ReadAsStreamAsync(ct);
        var read = await BodyReader.ReadLimited(body, SitemapLimits.MaxCompressedBytes, ct);
        return read.Truncated ? Failed : new SitemapDownload(status, read.Bytes);
    }
}
