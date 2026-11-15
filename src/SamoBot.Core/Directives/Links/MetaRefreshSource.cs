using System.Globalization;
using System.Text.RegularExpressions;

namespace SamoBot.Core.Directives.Links;

/// <summary>
/// &lt;meta http-equiv="refresh" content="5; url=/new"&gt; sends the browser to another URL, so it is treated as a redirect.
/// A refresh without a URL only reloads the page and is ignored.
/// </summary>
public sealed partial class MetaRefreshSource : IRedirectSource
{
    public IEnumerable<Uri> Targets(PageContext page) =>
        page.HttpEquiv("refresh").Select(tag => Target(tag.Content)).Select(page.Resolve).OfType<Uri>();

    /// <summary>The URL of a refresh value such as "0; URL='/next'", or null when it has none.</summary>
    public static string? Target(string? content)
    {
        var match = Refresh().Match(content ?? string.Empty);
        if (!match.Success || !decimal.TryParse(match.Groups["delay"].Value, NumberStyles.Number, CultureInfo.InvariantCulture, out _))
        {
            return null;
        }

        var url = match.Groups["url"].Value.Trim().Trim('\'', '"').Trim();
        return url.Length == 0 ? null : url;
    }

    [GeneratedRegex(@"^\s*(?<delay>\d+(\.\d*)?)\s*(?:[;,]\s*(?:url\s*=\s*)?(?<url>.*))?$", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex Refresh();
}
