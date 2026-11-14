using System.Text;

namespace SamoBot.Core.Search;

/// <summary>
/// Turns engine-highlighted text into a safe snippet. The engine wraps matches in two private-use characters,
/// which this class swaps for mark tags after it has encoded everything else. The result is encoded text plus mark tags only.
/// </summary>
public static class SnippetFormatter
{
    public const char HighlightStart = '';
    public const char HighlightEnd = '';

    private const string Open = "<mark>";
    private const string Close = "</mark>";

    /// <summary>Picks the first text that contains a match, else the first non-empty text, and formats it.</summary>
    public static string Pick(params string?[] highlighted)
    {
        var chosen = highlighted.FirstOrDefault(text => text is not null && text.Contains(HighlightStart))
                     ?? highlighted.FirstOrDefault(text => !string.IsNullOrWhiteSpace(text));
        return Format(chosen);
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

    /// <summary>Appends one character and returns whether a mark tag is open afterwards.</summary>
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
