using System.Net;
using HtmlAgilityPack;

namespace SamoBot.Adapters.Html;

/// <summary>The single-value metadata of a page.</summary>
internal static class PageMetadata
{
    public static string Title(HtmlDocument document) =>
        WebUtility.HtmlDecode(document.DocumentNode.SelectSingleNode("//title")?.InnerText?.Trim() ?? string.Empty);

    public static string Description(HtmlDocument document) => Attribute(document, "//meta[@name='description']", "content");

    public static string Keywords(HtmlDocument document) => Attribute(document, "//meta[@name='keywords']", "content");

    public static string Author(HtmlDocument document) => Attribute(document, "//meta[@name='author']", "content");

    public static string Canonical(HtmlDocument document) => Attribute(document, "//link[@rel='canonical']", "href");

    public static string Language(HtmlDocument document)
    {
        var lang = Attribute(document, "//html", "lang");
        return string.IsNullOrWhiteSpace(lang) ? Attribute(document, "//meta[@http-equiv='content-language']", "content") : lang;
    }

    internal static string Attribute(HtmlDocument document, string xpath, string attribute) =>
        document.DocumentNode.SelectSingleNode(xpath)?.GetAttributeValue(attribute, string.Empty) ?? string.Empty;
}
