using HtmlAgilityPack;

namespace SamoBot.Adapters.Html;

internal static class HtmlDocumentExtensions
{
    /// <summary>Selects nodes by XPath. HtmlAgilityPack returns null for no match. This returns an empty sequence.</summary>
    public static IEnumerable<HtmlNode> Nodes(this HtmlDocument document, string xpath) =>
        document.DocumentNode.SelectNodes(xpath) ?? Enumerable.Empty<HtmlNode>();
}
