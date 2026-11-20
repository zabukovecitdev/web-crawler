using Microsoft.Extensions.Options;
using SamoBot.Adapters.Browser;
using SamoBot.Core.Common;
using SamoBot.Core.Domain;
using SamoBot.Core.Directives;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Http;

public sealed class HttpFetcher(
    HttpClient httpClient,
    IOptions<ChromeRenderingOptions> chrome,
    IOptions<CrawlerNetworkOptions> network,
    ResponseHeaderSelection keptHeaders,
    TimeProvider timeProvider) : IFetcher
{
    public const string UserAgent = "SamoBot/1.0 (+https://github.com/zabukovecitdev/web-crawler)";
    public const int MaxBodyBytes = 5 * 1024 * 1024;

    public const string Accept = "text/html,application/xhtml+xml;q=0.9,text/markdown;q=0.8,text/plain;q=0.7,*/*;q=0.6";

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
        request.Headers.TryAddWithoutValidation("Accept", Accept);
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
            RedirectLocation = response.Headers.Location?.OriginalString,
            RetryAfter = RetryAfter(response),
            ResponseHeaders = keptHeaders.Collect(name => response.Headers.NonValidated.TryGetValues(name, out var values) ? values : null)
        };
    }

    private TimeSpan? RetryAfter(HttpResponseMessage response)
    {
        var header = response.Headers.RetryAfter;
        var wait = header?.Delta ?? (header?.Date - timeProvider.GetUtcNow());
        return wait is { } value && value < TimeSpan.Zero ? TimeSpan.Zero : wait;
    }

    private FetchResponse Failure(string error, long started) => new()
    {
        StatusCode = 0,
        Error = error,
        ResponseTimeMs = (long)timeProvider.GetElapsedTime(started).TotalMilliseconds
    };
}
