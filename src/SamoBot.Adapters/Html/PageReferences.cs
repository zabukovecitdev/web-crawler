using HtmlAgilityPack;
using SamoBot.Core.Domain;

namespace SamoBot.Adapters.Html;

/// <summary>The links and images of a page.</summary>
internal static class PageReferences
{
    public static IReadOnlyList<ParsedLink> Links(HtmlDocument document) =>
        document.Nodes("//a[@href]")
            .Select(ToLink)
            .Where(link => !string.IsNullOrWhiteSpace(link.Url))
            .ToList();

    public static IReadOnlyList<ParsedImage> Images(HtmlDocument document) =>
        document.Nodes("//img")
            .Select(ToImage)
            .Where(image => !string.IsNullOrWhiteSpace(image.Src))
            .ToList();

    private static ParsedLink ToLink(HtmlNode anchor)
    {
        var rel = anchor.GetAttributeValue("rel", string.Empty);
        return new ParsedLink
        {
            Url = anchor.GetAttributeValue("href", string.Empty),
            LinkText = PageText.Clean(anchor.InnerText),
            Rel = rel,
            Title = anchor.GetAttributeValue("title", string.Empty),
            IsNoFollow = rel.Contains("nofollow", StringComparison.OrdinalIgnoreCase)
        };
    }

    private static ParsedImage ToImage(HtmlNode image) => new()
    {
        Src = image.GetAttributeValue("src", string.Empty),
        Alt = image.GetAttributeValue("alt", string.Empty),
        Title = image.GetAttributeValue("title", string.Empty),
        Width = image.GetAttributeValue("width", string.Empty),
        Height = image.GetAttributeValue("height", string.Empty)
    };
}
