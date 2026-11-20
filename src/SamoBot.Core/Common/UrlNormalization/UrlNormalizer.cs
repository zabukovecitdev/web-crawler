using System.Web;

namespace SamoBot.Core.Common;

public static class UrlNormalizer
{
    private static readonly IUrlRule[] UrlRules = [new LowerCaseSchemeAndHost(), new DropFragment(), new DropDefaultPort()];

    private static readonly IQueryRule[] QueryRules =
        [new RemoveTrackingParameters(TrackingParameters.Names, TrackingParameters.UtmPrefix), new SortParameters()];

    public static bool TryClean(string url, out Uri? normalizedUrl)
    {
        if (string.IsNullOrWhiteSpace(url) || !Uri.TryCreate(url, UriKind.Absolute, out var uri))
        {
            normalizedUrl = null;
            return false;
        }

        normalizedUrl = Normalize(uri);
        return true;
    }

    private static Uri Normalize(Uri uri)
    {
        var builder = new UriBuilder(uri);
        foreach (var rule in UrlRules)
        {
            rule.Apply(builder);
        }

        if (!string.IsNullOrEmpty(builder.Query))
        {
            builder.Query = NormalizeQuery(builder.Query);
        }

        return builder.Uri;
    }

    private static string NormalizeQuery(string query)
    {
        var parameters = HttpUtility.ParseQueryString(query);
        var names = QueryRules.Aggregate(parameters.AllKeys.OfType<string>(), (current, rule) => rule.Apply(current));

        var normalized = HttpUtility.ParseQueryString(string.Empty);
        foreach (var name in names)
        {
            normalized[name] = parameters[name];
        }

        return normalized.ToString() ?? string.Empty;
    }
}
