using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Core.Jobs;

namespace SamoBot.Core.SiteChecks;

public sealed class StartPageResolver(FetcherResolver fetchers, RobotsGate robots)
{
    public const int MaxHops = 5;

    public async Task<StartPage> Resolve(Uri start, CancellationToken ct)
    {
        var current = start;
        for (var hop = 0; hop < MaxHops; hop++)
        {
            var redirected = hop > 0;
            var rules = await robots.RulesFor(current.Authority, ct);
            if (!rules.IsAllowed(current.PathAndQuery, RobotsGate.UserAgent))
            {
                return StartPage.Blocked(current, redirected);
            }

            var response = await fetchers.For(ProbeUrl.For(current, false)).Fetch(ProbeUrl.For(current, false), ct);
            var next = Next(current, response);
            if (next is null)
            {
                return Answer(current, redirected, response);
            }

            current = next;
        }

        return StartPage.Failed(current, true, "The address redirects too many times.");
    }

    private static Uri? Next(Uri current, FetchResponse response)
    {
        if (response.StatusCode is < 300 or > 399 || string.IsNullOrWhiteSpace(response.RedirectLocation))
        {
            return null;
        }

        var found = Uri.TryCreate(current, response.RedirectLocation, out var target);
        return found && target!.Scheme is "http" or "https" && HostRules.Check(target.DnsSafeHost) is null ? target : null;
    }

    private static StartPage Answer(Uri url, bool redirected, FetchResponse response) =>
        new(url, redirected, response.StatusCode, Problem(response), false, response.Content, response.ContentType);

    private static string? Problem(FetchResponse response)
    {
        if (response.IsBlocked)
        {
            return "The address is local, private or reserved.";
        }

        return response.StatusCode == 0 ? response.Error ?? "The site did not answer." : null;
    }
}
