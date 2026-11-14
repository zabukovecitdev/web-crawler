using System.Net.Sockets;
using Microsoft.Playwright;
using SamoBot.Adapters.Http;

namespace SamoBot.Adapters.Browser;

/// <summary>
/// Validates the host of every request that a browser context makes (navigation, redirect hops, sub-requests, popups)
/// with the address policy. A blocked request is aborted and every WebSocket is refused. Chrome resolves the name again
/// when it connects, so a short rebinding window remains, and a worker may still open a socket the route does not see:
/// the network policy of the Chrome container (no route to private ranges) is the control for both.
/// </summary>
internal sealed class BrowserRequestGuard(SafeResolver resolver, IBrowserContext context, IPage page, CancellationToken ct)
{
    /// <summary>True when the page itself (not only a sub-resource) was refused.</summary>
    public bool PageBlocked { get; private set; }

    public async Task Attach()
    {
        await context.RouteAsync("**/*", Handle);
        await context.RouteWebSocketAsync("**/*", socket => socket.CloseAsync(new WebSocketRouteCloseOptions { Code = 1008, Reason = "blocked" }));
    }

    internal static bool IsLocalScheme(Uri uri) => uri.Scheme is "data" or "blob" or "about";

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
        IsLocalScheme(uri) ? route.ContinueAsync() : route.AbortAsync();

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
