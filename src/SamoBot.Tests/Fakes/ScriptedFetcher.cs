using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class ScriptedFetcher(bool forBrowser, Func<string, FetchResponse> respond) : IFetcher
{
    public List<string> Requested { get; } = [];

    public bool Supports(ClaimedUrl url) => url.UseJsRendering == forBrowser;

    public Task<FetchResponse> Fetch(ClaimedUrl url, CancellationToken ct)
    {
        Requested.Add(url.Url);
        return Task.FromResult(respond(url.Url));
    }

    public static FetchResponse Html(string body = "<html></html>", int status = 200) => new()
    {
        StatusCode = status,
        ContentType = "text/html",
        Content = System.Text.Encoding.UTF8.GetBytes(body)
    };

    public static FetchResponse Redirect(string location, int status = 301) => new() { StatusCode = status, RedirectLocation = location };
}
