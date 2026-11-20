using System.Globalization;
using System.Text.RegularExpressions;

namespace SamoBot.Core.Directives.Links;

public sealed partial class MetaRefreshSource : IRedirectSource
{
    public IEnumerable<Uri> Targets(PageContext page) =>
        page.HttpEquiv("refresh").Select(tag => Target(tag.Content)).Select(page.Resolve).OfType<Uri>();

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
