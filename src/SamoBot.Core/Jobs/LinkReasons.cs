using System.Globalization;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;

namespace SamoBot.Core.Jobs;

public static class LinkReasons
{
    public const string Robots = "robots";
    private const string HttpPrefix = "http-";
    private const string MaxFailures = "max-failures";

    public static string Label(string? reason)
    {
        if (string.IsNullOrEmpty(reason))
        {
            return "Unknown error";
        }

        if (reason.StartsWith(HttpPrefix, StringComparison.Ordinal))
        {
            return $"HTTP {reason[HttpPrefix.Length..]}";
        }

        return reason switch
        {
            "timeout" => "Timed out",
            MaxFailures => $"Gave up after {OutcomeClassifier.MaxFailures} attempts",
            "network-error" => "Network error",
            _ => reason
        };
    }

    public static string Failed(JobLinkFacts link)
    {
        if (link.FailReason == MaxFailures)
        {
            return $"Gave up after {OutcomeClassifier.MaxFailures} failed attempts. The last answer was {LastAnswer(link)}.";
        }

        return link.StatusCode is > 0
            ? $"The server answered {Code(link)}, so there is no page to store."
            : $"The fetch failed: {Label(link.FailReason)}.";
    }

    public static string Retrying(JobLinkFacts link) =>
        $"The last fetch failed ({LastAnswer(link)}). Attempt {link.FailCount} of {OutcomeClassifier.MaxFailures}; it is tried again automatically.";

    public static string Skipped(JobLinkFacts link) => link.FailReason switch
    {
        Robots => "robots.txt of the site disallows this URL, so it was not requested.",
        UrlFailReasons.Unselected => "You unticked its folder before it was fetched, so it was not requested.",
        Blocked.Reason => "The host points at a private or reserved address, which the crawler never requests.",
        Redirected.Reason => $"The URL redirects ({Code(link)}). The target is crawled as its own link.",
        _ => NotStored(link)
    };

    public static string NotIndexed(JobLinkFacts link) =>
        link.Policy is { Directives.Count: > 0 } policy
            ? $"Kept out of the search index because the site asked for it: {string.Join(", ", policy.Directives)}."
            : "Kept out of the search index by the directives of the page.";

    private static string NotStored(JobLinkFacts link) =>
        IsHtml(link.ContentType)
            ? "The response had no body, so nothing was stored."
            : $"Not an HTML page ({link.ContentType}). Only HTML pages are stored and indexed.";

    private static bool IsHtml(string? contentType) =>
        contentType is null || contentType.Contains("html", StringComparison.OrdinalIgnoreCase);

    private static string LastAnswer(JobLinkFacts link) =>
        link.StatusCode is > 0 ? $"HTTP {Code(link)}" : "no response (timeout or network error)";

    private static string Code(JobLinkFacts link) => (link.StatusCode ?? 0).ToString(CultureInfo.InvariantCulture);
}
