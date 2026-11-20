using System.Net;
using HtmlAgilityPack;
using SamoBot.Core.Domain;

namespace SamoBot.Adapters.Html;

internal static class PageMetadata
{
    public static string Title(HtmlDocument document) =>
        WebUtility.HtmlDecode(document.DocumentNode.SelectSingleNode("//title")?.InnerText?.Trim() ?? string.Empty);

    public static string Description(HtmlDocument document) => Attribute(document, "//meta[@name='description']", "content");

    public static string Keywords(HtmlDocument document) => Attribute(document, "//meta[@name='keywords']", "content");

    public static string Author(HtmlDocument document) => Attribute(document, "//meta[@name='author']", "content");

    public static string Canonical(HtmlDocument document) =>
        document.Nodes("//link[@rel and @href]")
            .FirstOrDefault(link => HasToken(link.GetAttributeValue("rel", string.Empty), "canonical"))
            ?.GetAttributeValue("href", string.Empty) ?? string.Empty;

    public static string BaseHref(HtmlDocument document) => Attribute(document, "//base[@href]", "href");

    public static IReadOnlyList<MetaTag> MetaTags(HtmlDocument document) =>
        document.Nodes("//meta[@name or @http-equiv]")
            .Select(ToMetaTag)
            .Where(tag => tag.Name.Length > 0)
            .ToList();

    private static MetaTag ToMetaTag(HtmlNode node)
    {
        var content = WebUtility.HtmlDecode(node.GetAttributeValue("content", string.Empty)).Trim();
        var httpEquiv = node.GetAttributeValue("http-equiv", string.Empty).Trim();
        return httpEquiv.Length > 0
            ? new MetaTag(httpEquiv, content, HttpEquiv: true)
            : new MetaTag(node.GetAttributeValue("name", string.Empty).Trim(), content);
    }

    private static bool HasToken(string value, string token) =>
        value.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries).Any(part => part.Equals(token, StringComparison.OrdinalIgnoreCase));

    public static string Language(HtmlDocument document)
    {
        var lang = Attribute(document, "//html", "lang");
        return string.IsNullOrWhiteSpace(lang) ? Attribute(document, "//meta[@http-equiv='content-language']", "content") : lang;
    }

    internal static string Attribute(HtmlDocument document, string xpath, string attribute) =>
        document.DocumentNode.SelectSingleNode(xpath)?.GetAttributeValue(attribute, string.Empty) ?? string.Empty;
}
