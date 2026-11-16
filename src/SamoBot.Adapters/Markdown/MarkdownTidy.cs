using System.Text.RegularExpressions;

namespace SamoBot.Adapters.Markdown;

/// <summary>Normalises Markdown text: line endings, trailing spaces, blank lines and empty links and bullets.</summary>
internal static partial class MarkdownTidy
{
    private static readonly string[] EmptyLines = ["[]()", "![]()", "-"];

    public static string Normalize(string markdown)
    {
        var lines = markdown.Replace("\r\n", "\n").Split('\n')
            .Select(line => line.TrimEnd())
            .Where(line => !EmptyLines.Contains(line.Trim()));
        var text = ManyNewlines().Replace(string.Join('\n', lines), "\n\n").Trim();
        return text.Length == 0 ? string.Empty : text + "\n";
    }

    [GeneratedRegex("\n{3,}")]
    private static partial Regex ManyNewlines();
}
