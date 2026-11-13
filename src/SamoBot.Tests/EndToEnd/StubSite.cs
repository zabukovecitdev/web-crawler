using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace SamoBot.Tests.EndToEnd;

/// <summary>
/// A small site on a random local port with 22 URLs: a robots.txt that blocks /private, one page that always returns 500,
/// one redirect, a root-relative link, and two pages that link to each other.
/// </summary>
public sealed class StubSite : IAsyncDisposable
{
    public const int NormalPageCount = 14;

    private readonly WebApplication _app;
    private readonly List<string> _requested = [];

    private StubSite(WebApplication app, string baseUrl)
    {
        _app = app;
        BaseUrl = baseUrl;
    }

    public string BaseUrl { get; }

    public string Authority => new Uri(BaseUrl).Authority;

    public IReadOnlyList<string> Requested
    {
        get
        {
            lock (_requested)
            {
                return _requested.ToList();
            }
        }
    }

    /// <summary>The paths that must be fetched with status 200 and must reach the search index.</summary>
    public static IReadOnlyList<string> SuccessfulPaths { get; } =
        ["/", .. Enumerable.Range(1, NormalPageCount).Select(i => $"/page/{i}"), "/a", "/b", "/new", "/about"];

    public static async Task<StubSite> Start()
    {
        var baseUrl = $"http://127.0.0.1:{FreePort()}";
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls(baseUrl);
        builder.Logging.ClearProviders();

        var site = new StubSite(builder.Build(), baseUrl);
        site.MapRoutes();
        await site._app.StartAsync();
        return site;
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }

    private static int FreePort()
    {
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private void MapRoutes()
    {
        _app.Use(async (context, next) =>
        {
            lock (_requested)
            {
                _requested.Add(context.Request.Path.Value ?? string.Empty);
            }

            await next();
        });

        _app.MapGet("/robots.txt", () => Results.Text("User-agent: *\nDisallow: /private\n"));
        _app.MapGet("/", () => Html("Home", RootLinks()));
        _app.MapGet("/page/{n:int}", (int n) => Html($"Page {n}", $"""<p>Content of page {n}.</p>"""));
        _app.MapGet("/a", () => Html("Page A", """<a href="/b">to b</a>"""));
        _app.MapGet("/b", () => Html("Page B", """<a href="/a">to a</a>"""));
        _app.MapGet("/about", () => Html("About", "<p>About us.</p>"));
        _app.MapGet("/new", () => Html("New home", "<p>The redirect target.</p>"));
        _app.MapGet("/old", () => Results.Redirect("/new", permanent: true));
        _app.MapGet("/boom", () => Results.StatusCode(500));
        _app.MapGet("/private/secret", () => Html("Secret", "<p>Nobody may crawl this.</p>"));
    }

    private static string RootLinks()
    {
        var pages = string.Join(string.Empty, Enumerable.Range(1, NormalPageCount).Select(i => $"""<a href="/page/{i}">p{i}</a>"""));
        return pages + """<a href="/private/secret">s</a><a href="/boom">b</a><a href="/old">o</a><a href="/about">about</a><a href="/a">a</a>""";
    }

    private static IResult Html(string title, string body) =>
        Results.Content($"<!DOCTYPE html><html lang=\"en\"><head><title>{title}</title></head><body><h1>{title}</h1>{body}</body></html>", "text/html");
}
