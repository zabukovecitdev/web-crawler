using System.Net.Sockets;
using Microsoft.Playwright;
using SamoBot.Adapters.Http;

namespace SamoBot.Adapters.Browser;

internal sealed class BrowserRequestGuard(SafeResolver resolver, IBrowserContext context, IPage page, CancellationToken ct)
{
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
            return false;
        }
    }
}
