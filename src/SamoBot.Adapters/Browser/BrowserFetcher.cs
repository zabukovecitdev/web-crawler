using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Playwright;
using SamoBot.Adapters.Http;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Browser;

/// <summary>Renders a page in a Chrome browser that Playwright reaches over CDP. A render error gives status code 0.</summary>
public sealed partial class BrowserFetcher(
    IOptions<ChromeRenderingOptions> options,
    SafeResolver resolver,
    TimeProvider timeProvider,
    ILogger<BrowserFetcher> logger) : IFetcher, IAsyncDisposable
{
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private IPlaywright? _playwright;
    private IBrowser? _browser;

    public bool Supports(ClaimedUrl url) => url.UseJsRendering && options.Value.IsAvailable;

    public async Task<FetchResponse> Fetch(ClaimedUrl url, CancellationToken ct)
    {
        var started = timeProvider.GetTimestamp();
        try
        {
            var browser = await EnsureBrowser(ct);
            return await Render(browser, url, started, ct);
        }
        catch (PlaywrightException ex)
        {
            LogRenderFailed(ex, url.Url);
            return new FetchResponse
            {
                StatusCode = 0,
                Error = ex.Message,
                ResponseTimeMs = (long)timeProvider.GetElapsedTime(started).TotalMilliseconds
            };
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_browser is not null)
        {
            await _browser.CloseAsync();
            _browser = null;
        }

        _playwright?.Dispose();
        _playwright = null;
        _initLock.Dispose();
    }

    private async Task<FetchResponse> Render(IBrowser browser, ClaimedUrl url, long started, CancellationToken ct)
    {
        var page = await browser.NewPageAsync();
        var guard = new BrowserRequestGuard(resolver, page, ct);
        try
        {
            await guard.Attach();
            return await Navigate(page, guard, url, started);
        }
        catch (PlaywrightException) when (guard.PageBlocked)
        {
            return FetchResponse.BlockedAddress();
        }
        finally
        {
            await page.CloseAsync();
        }
    }

    private async Task<FetchResponse> Navigate(IPage page, BrowserRequestGuard guard, ClaimedUrl url, long started)
    {
        var response = await page.GotoAsync(url.Url, new PageGotoOptions
        {
            Timeout = options.Value.DefaultTimeoutMs,
            WaitUntil = WaitUntilState.NetworkIdle
        });
        if (guard.PageBlocked)
        {
            return FetchResponse.BlockedAddress();
        }

        var html = await page.ContentAsync();
        return new FetchResponse
        {
            StatusCode = response?.Status ?? 0,
            ContentType = "text/html; charset=utf-8",
            Content = Encoding.UTF8.GetBytes(html),
            ResponseTimeMs = (long)timeProvider.GetElapsedTime(started).TotalMilliseconds
        };
    }

    private async Task<IBrowser> EnsureBrowser(CancellationToken ct)
    {
        await _initLock.WaitAsync(ct);
        try
        {
            if (_browser is null)
            {
                _playwright = await Playwright.CreateAsync();
                var endpoint = options.Value.CdpEndpoint!;
                _browser = await _playwright.Chromium.ConnectOverCDPAsync(endpoint);
                LogConnected(endpoint);
            }

            return _browser;
        }
        finally
        {
            _initLock.Release();
        }
    }

    [LoggerMessage(EventId = 1, Level = LogLevel.Warning, Message = "JS render failed for {Url}")]
    private partial void LogRenderFailed(Exception exception, string url);

    [LoggerMessage(EventId = 2, Level = LogLevel.Information, Message = "Connected Playwright to CDP at {Endpoint}")]
    private partial void LogConnected(string endpoint);
}
