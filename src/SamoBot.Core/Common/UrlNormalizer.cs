using System.Web;
using FluentResults;

namespace SamoBot.Core.Common;

public static class UrlNormalizer
{
    // Note: This URL normalization could be done with some sort of pipeline which can be easily extended and configured
    private static readonly HashSet<string> TrackingParameters = new(StringComparer.OrdinalIgnoreCase)
    {
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content",
        "utm_id",
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
        "clickId",
        "click_id",
        "affiliate",
        "affiliateId",
        "affiliate_id",
        "partner",
        "partnerId",
        "partner_id",
    };

    public static Result<Uri> Clean(string url)
    {
        if (string.IsNullOrWhiteSpace(url))
        {
            return Result.Fail("Invalid URL");
        }

        if (!Uri.TryCreate(url, UriKind.Absolute, out var cleanedUri))
        {
            return Result.Fail("Invalid URL");
        }

        return Result.Ok(cleanedUri.Normalize());
    }

    public static bool TryClean(string url, out Uri? normalizedUrl)
    {
        if (string.IsNullOrWhiteSpace(url) || !Uri.TryCreate(url, UriKind.Absolute, out var uri))
        {
            normalizedUrl = null;
            return false;
        }

        normalizedUrl = uri.NormalizeAndRemoveTracking();
        return true;
    }

    private static Uri Normalize(this Uri uri) => Rebuild(uri, removeTracking: false);

    private static Uri NormalizeAndRemoveTracking(this Uri uri) => Rebuild(uri, removeTracking: true);

    private static Uri Rebuild(Uri uri, bool removeTracking)
    {
        var builder = new UriBuilder(uri)
        {
            Scheme = uri.Scheme.ToLowerInvariant(),
            Host = uri.Host.ToLowerInvariant(),
            Fragment = string.Empty
        };

        if (IsDefaultPort(builder))
        {
            builder.Port = -1;
        }

        // Keep trailing slashes as-is for web crawler accuracy
        // Some sites treat /page and /page/ as different resources
        if (!string.IsNullOrEmpty(builder.Query))
        {
            builder.Query = SortQuery(builder.Query, removeTracking);
        }

        return builder.Uri;
    }

    private static bool IsDefaultPort(UriBuilder builder) =>
        (builder.Scheme == "http" && builder.Port == 80) ||
        (builder.Scheme == "https" && builder.Port == 443);

    private static string SortQuery(string query, bool removeTracking)
    {
        var parameters = HttpUtility.ParseQueryString(query);
        var keys = parameters.AllKeys
            .Where(key => key != null && !(removeTracking && IsTrackingParameter(key)))
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
