using System.Net.Sockets;
using Microsoft.Playwright;
using SamoBot.Adapters.Http;

namespace SamoBot.Adapters.Browser;

/// <summary>
/// Validates the host of every request that one page makes (navigation, redirect hops, sub-requests) with the address policy.
/// A blocked request is aborted. Chrome resolves the name again when it connects, so a short rebinding window remains.
/// </summary>
internal sealed class BrowserRequestGuard(SafeResolver resolver, IPage page, CancellationToken ct)
{
    /// <summary>True when the page itself (not only a sub-resource) was refused.</summary>
    public bool PageBlocked { get; private set; }

    public Task Attach() => page.RouteAsync("**/*", Handle);

    private async Task Handle(IRoute route)
    {
        var request = route.Request;
        if (!Uri.TryCreate(request.Url, UriKind.Absolute, out var uri))
        {
            await route.AbortAsync();
            return;
        }

        if (uri.Scheme is not ("http" or "https"))
        {
            await Allow(route, uri);
            return;
        }

        if (await IsHostAllowed(uri.DnsSafeHost))
        {
            await route.ContinueAsync();
            return;
        }

        PageBlocked |= request.IsNavigationRequest && ReferenceEquals(request.Frame, page.MainFrame);
        await route.AbortAsync();
    }

    // data: and blob: need no network. Every other scheme (file:, ftp:, ws: ...) is refused.
    private static Task Allow(IRoute route, Uri uri) =>
        uri.Scheme is "data" or "blob" or "about" ? route.ContinueAsync() : route.AbortAsync();

    private async Task<bool> IsHostAllowed(string host)
    {
        try
        {
            return await resolver.AllAllowed(host, ct);
        }
        catch (SocketException)
        {
            // Our resolver cannot resolve it. Refuse the request, but do not call the page blocked.
            return false;
        }
    }
}
