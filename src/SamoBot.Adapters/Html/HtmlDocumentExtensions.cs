using HtmlAgilityPack;

namespace SamoBot.Adapters.Html;

internal static class HtmlDocumentExtensions
{
    public static IEnumerable<HtmlNode> Nodes(this HtmlDocument document, string xpath) =>
        document.DocumentNode.SelectNodes(xpath) ?? Enumerable.Empty<HtmlNode>();
}
