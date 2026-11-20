using System.Text;

namespace SamoBot.Core.Search;

public static class SnippetFormatter
{
    public const char HighlightStart = '';
    public const char HighlightEnd = '';

    private const string Open = "<mark>";
    private const string Close = "</mark>";

    public static string Pick(params string?[] highlighted) => PickWithin(null, highlighted);

    public static string PickWithin(int? maxLength, params string?[] highlighted)
    {
        var chosen = highlighted.FirstOrDefault(text => text is not null && text.Contains(HighlightStart))
                     ?? highlighted.FirstOrDefault(text => !string.IsNullOrWhiteSpace(text));
        return Format(maxLength is { } limit ? Limit(chosen, limit) : chosen);
    }

    public static string? Limit(string? highlighted, int maxLength)
    {
        if (highlighted is null || maxLength <= 0)
        {
            return maxLength <= 0 ? null : highlighted;
        }

        var visible = 0;
        for (var i = 0; i < highlighted.Length; i++)
        {
            if (highlighted[i] is HighlightStart or HighlightEnd)
            {
                continue;
            }

            if (++visible > maxLength)
            {
                return highlighted[..i];
            }
        }

        return highlighted;
    }

    public static string Format(string? highlighted)
    {
        if (string.IsNullOrEmpty(highlighted))
        {
            return string.Empty;
        }

        var builder = new StringBuilder(highlighted.Length + 16);
        var open = false;
        foreach (var c in highlighted)
        {
            open = Append(builder, c, open);
        }

        if (open)
        {
            builder.Append(Close);
        }

        return builder.ToString().Trim();
    }

    private static bool Append(StringBuilder builder, char c, bool open)
    {
        if (c == HighlightStart && !open)
        {
            builder.Append(Open);
            return true;
        }

        if (c == HighlightEnd && open)
        {
            builder.Append(Close);
            return false;
        }

        if (c != HighlightStart && c != HighlightEnd)
        {
            builder.Append(Encode(c));
        }

        return open;
    }

    private static string Encode(char c) => c switch
    {
        '&' => "&amp;",
        '<' => "&lt;",
        '>' => "&gt;",
        '"' => "&quot;",
        '\'' => "&#39;",
        _ when char.IsControl(c) || char.IsWhiteSpace(c) => " ",
        _ => c.ToString()
    };
}
