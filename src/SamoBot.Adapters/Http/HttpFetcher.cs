using Microsoft.Extensions.Options;
using SamoBot.Adapters.Browser;
using SamoBot.Core.Common;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Http;

/// <summary>
/// Downloads a URL with a plain HTTP request. It does not follow redirects. The frontier decides what to do with them.
/// A network error or a timeout gives status code 0. The fetcher does not throw for them.
/// A <see cref="RequestDeadline"/> limits the time to the first byte and the total time, the body read included.
/// A cancellation by the caller (shutdown) is not a timeout and still throws.
/// </summary>
public sealed class HttpFetcher(
    HttpClient httpClient,
    IOptions<ChromeRenderingOptions> chrome,
    IOptions<CrawlerNetworkOptions> network,
    TimeProvider timeProvider) : IFetcher
{
    public const string UserAgent = "SamoBot/1.0 (+https://github.com/zabukovecitdev/web-crawler)";
    public const int MaxBodyBytes = 5 * 1024 * 1024;

    public bool Supports(ClaimedUrl url) => !url.UseJsRendering || !chrome.Value.IsAvailable;

    public async Task<FetchResponse> Fetch(ClaimedUrl url, CancellationToken ct)
    {
        var started = timeProvider.GetTimestamp();
        var limits = network.Value;
        using var deadline = new RequestDeadline(
            RequestDeadline.Seconds(limits.RequestTimeoutSeconds),
            RequestDeadline.Seconds(limits.FirstByteTimeoutSeconds),
            timeProvider,
            ct);
        try
        {
            return await Send(url, started, deadline);
        }
        catch (HttpRequestException ex)
        {
            return FromHttpError(ex, started);
        }
        catch (IOException ex)
        {
            return Failure(ex.Message, started);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return Failure("timeout", started);
        }
    }

    private FetchResponse FromHttpError(HttpRequestException ex, long started)
    {
        if (BlockedAddressException.IsCause(ex))
        {
            return FetchResponse.BlockedAddress((long)timeProvider.GetElapsedTime(started).TotalMilliseconds);
        }

        return Failure(RequestDeadline.IsConnectTimeout(ex) ? "timeout" : ex.Message, started);
    }

    private async Task<FetchResponse> Send(ClaimedUrl url, long started, RequestDeadline deadline)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, url.Url);
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, deadline.Token);
        deadline.HeadersReceived();

        await using var body = await response.Content.ReadAsStreamAsync(deadline.Token);
        var content = await BodyReader.ReadLimited(body, MaxBodyBytes, deadline.Token);

        return new FetchResponse
        {
            StatusCode = (int)response.StatusCode,
            ContentType = response.Content.Headers.ContentType?.ToString(),
            Content = content,
            ResponseTimeMs = (long)timeProvider.GetElapsedTime(started).TotalMilliseconds,
            RedirectLocation = response.Headers.Location?.OriginalString
        };
    }

    private FetchResponse Failure(string error, long started) => new()
    {
        StatusCode = 0,
        Error = error,
        ResponseTimeMs = (long)timeProvider.GetElapsedTime(started).TotalMilliseconds
    };
}
