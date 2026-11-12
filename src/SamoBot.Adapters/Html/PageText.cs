using System.Net;
using System.Text.RegularExpressions;
using HtmlAgilityPack;
using SamoBot.Core.Domain;

namespace SamoBot.Adapters.Html;

/// <summary>The readable text of a page: headings and body text.</summary>
internal static partial class PageText
{
    private static readonly string[] UnwantedTags = ["script", "style", "noscript", "svg", "path"];

    public static IReadOnlyList<ParsedHeading> Headings(HtmlDocument document)
    {
        var headings = new List<ParsedHeading>();
        for (var level = 1; level <= 6; level++)
        {
            headings.AddRange(HeadingsOfLevel(document.Nodes($"//h{level}"), level));
        }

        return headings;
    }

    public static string BodyText(HtmlDocument document)
    {
        var copy = new HtmlDocument();
        copy.LoadHtml(document.DocumentNode.OuterHtml);
        RemoveNodes(copy, node => UnwantedTags.Contains(node.Name) || node.NodeType == HtmlNodeType.Comment);

        var body = copy.DocumentNode.SelectSingleNode("//body");
        if (body is null)
        {
            return string.Empty;
        }

        var text = Whitespace().Replace(WebUtility.HtmlDecode(body.InnerText), " ");
        return EmptyLines().Replace(text, "\n\n").Trim();
    }

    internal static string Clean(string? text) => Whitespace().Replace(WebUtility.HtmlDecode(text?.Trim() ?? string.Empty), " ");

    private static IEnumerable<ParsedHeading> HeadingsOfLevel(IEnumerable<HtmlNode> nodes, int level) =>
        nodes
            .Select(node => WebUtility.HtmlDecode(node.InnerText.Trim()))
            .Where(text => !string.IsNullOrWhiteSpace(text))
            .Select(text => new ParsedHeading(level, Whitespace().Replace(text, " ")));

    private static void RemoveNodes(HtmlDocument document, Func<HtmlNode, bool> shouldRemove)
    {
        foreach (var node in document.DocumentNode.Descendants().Where(shouldRemove).ToList())
        {
            node.Remove();
        }
    }

    [GeneratedRegex(@"\s+")]
    private static partial Regex Whitespace();

    [GeneratedRegex(@"(\r?\n\s*){2,}")]
    private static partial Regex EmptyLines();
}
