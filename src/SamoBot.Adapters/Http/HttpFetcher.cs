using Microsoft.Extensions.Options;
using SamoBot.Adapters.Browser;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Http;

/// <summary>
/// Downloads a URL with a plain HTTP request. It does not follow redirects. The frontier decides what to do with them.
/// A network error or a timeout gives status code 0. The fetcher does not throw for them.
/// </summary>
public sealed class HttpFetcher(HttpClient httpClient, IOptions<ChromeRenderingOptions> chrome, TimeProvider timeProvider) : IFetcher
{
    public const string UserAgent = "SamoBot/1.0 (+https://github.com/zabukovecitdev/web-crawler)";
    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);
    public const int MaxBodyBytes = 5 * 1024 * 1024;

    public bool Supports(ClaimedUrl url) => !url.UseJsRendering || !chrome.Value.IsAvailable;

    public async Task<FetchResponse> Fetch(ClaimedUrl url, CancellationToken ct)
    {
        var started = timeProvider.GetTimestamp();
        try
        {
            return await Send(url, started, ct);
        }
        catch (HttpRequestException ex)
        {
            return Failure(ex.Message, started);
        }
        catch (IOException ex)
        {
            return Failure(ex.Message, started);
        }
        catch (TaskCanceledException) when (!ct.IsCancellationRequested)
        {
            return Failure("timeout", started);
        }
    }

    private async Task<FetchResponse> Send(ClaimedUrl url, long started, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, url.Url);
        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);

        await using var body = await response.Content.ReadAsStreamAsync(ct);
        var content = await BodyReader.ReadLimited(body, MaxBodyBytes, ct);

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
