using System.Web;

namespace SamoBot.Core.Common;

public static class UrlNormalizer
{
    // Note: This URL normalization could be done with some sort of pipeline which can be easily extended and configured
    // Every utm_ parameter is tracking, see IsTrackingParameter. The set ignores case.
    private static readonly HashSet<string> TrackingParameters = new(StringComparer.OrdinalIgnoreCase)
    {
        "fbclid",
        "gclid",
        "igshid",
        "twclid",
        "li_fat_id",
        "_ga",
        "_gl",
        "yclid",
        "mc_cid",
        "mc_eid",
        "mkt_tok",
        "_hsenc",
        "_hsmi",
        "hsCtaTracking",
        "trk",
        "trk_info",
        "ncid",
        "clickid",
        "click_id",
        "affiliate",
        "affiliateId",
        "affiliate_id",
        "partner",
        "partnerId",
        "partner_id",
    };

    public static bool TryClean(string url, out Uri? normalizedUrl)
    {
        if (string.IsNullOrWhiteSpace(url) || !Uri.TryCreate(url, UriKind.Absolute, out var uri))
        {
            normalizedUrl = null;
            return false;
        }

        normalizedUrl = Rebuild(uri);
        return true;
    }

    private static Uri Rebuild(Uri uri)
    {
        var builder = new UriBuilder(uri)
        {
            Scheme = uri.Scheme.ToLowerInvariant(),
            Host = uri.Host.ToLowerInvariant(),
            Fragment = string.Empty
        };

        // builder.Uri drops the default port of the scheme by itself.
        // Keep trailing slashes as-is for web crawler accuracy
        // Some sites treat /page and /page/ as different resources
        if (!string.IsNullOrEmpty(builder.Query))
        {
            builder.Query = SortQuery(builder.Query);
        }

        return builder.Uri;
    }

    private static string SortQuery(string query)
    {
        var parameters = HttpUtility.ParseQueryString(query);
        var keys = parameters.AllKeys
            .Where(key => key != null && !IsTrackingParameter(key))
            .OrderBy(key => key, StringComparer.InvariantCultureIgnoreCase);

        var normalized = HttpUtility.ParseQueryString(string.Empty);
        foreach (var key in keys)
        {
            normalized[key] = parameters[key];
        }

        return normalized.ToString() ?? string.Empty;
    }

    private static bool IsTrackingParameter(string? parameterName)
    {
        if (string.IsNullOrEmpty(parameterName))
        {
            return false;
        }

        return TrackingParameters.Contains(parameterName) ||
               parameterName.StartsWith("utm_", StringComparison.OrdinalIgnoreCase);
    }
}
