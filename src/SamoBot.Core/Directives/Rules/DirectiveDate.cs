using System.Globalization;

namespace SamoBot.Core.Directives.Rules;

public static class DirectiveDate
{
    private static readonly Dictionary<string, TimeSpan> Zones = new(StringComparer.OrdinalIgnoreCase)
    {
        ["UTC"] = TimeSpan.Zero, ["GMT"] = TimeSpan.Zero, ["Z"] = TimeSpan.Zero, ["UT"] = TimeSpan.Zero,
        ["EST"] = TimeSpan.FromHours(-5), ["EDT"] = TimeSpan.FromHours(-4),
        ["CST"] = TimeSpan.FromHours(-6), ["CDT"] = TimeSpan.FromHours(-5),
        ["MST"] = TimeSpan.FromHours(-7), ["MDT"] = TimeSpan.FromHours(-6),
        ["PST"] = TimeSpan.FromHours(-8), ["PDT"] = TimeSpan.FromHours(-7),
        ["CET"] = TimeSpan.FromHours(1), ["CEST"] = TimeSpan.FromHours(2)
    };

    private static readonly string[] Formats =
    [
        "dddd, dd-MMM-yy HH:mm:ss", "ddd, dd MMM yyyy HH:mm:ss", "dd MMM yyyy HH:mm:ss", "d MMM yyyy HH:mm:ss",
        "dd MMM yyyy", "d MMM yyyy", "yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd"
    ];

    public static bool TryParse(string? text, out DateTimeOffset value)
    {
        value = default;
        if (string.IsNullOrWhiteSpace(text))
        {
            return false;
        }

        var (rest, zone) = SplitZone(text.Trim());
        if (zone is not null && DateTime.TryParseExact(rest, Formats, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var local))
        {
            value = new DateTimeOffset(DateTime.SpecifyKind(local, DateTimeKind.Unspecified), zone.Value);
            return true;
        }

        return DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out value)
               || TryExact(text, out value);
    }

    private static bool TryExact(string text, out DateTimeOffset value) =>
        DateTimeOffset.TryParseExact(text.Trim(), Formats, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AllowWhiteSpaces, out value);

    private static (string Remainder, TimeSpan? Zone) SplitZone(string text)
    {
        var space = text.LastIndexOf(' ');
        return space > 0 && Zones.TryGetValue(text[(space + 1)..], out var offset) ? (text[..space], offset) : (text, null);
    }
}
