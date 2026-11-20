using HtmlAgilityPack;
using SamoBot.Core.Domain;

namespace SamoBot.Adapters.Html;

internal static class StructuredData
{
    public static IReadOnlyDictionary<string, string> OpenGraph(HtmlDocument document) =>
        MetaPairs(document, "//meta[starts-with(@property, 'og:')]", "property");

    public static IReadOnlyDictionary<string, string> TwitterCard(HtmlDocument document) =>
        MetaPairs(document, "//meta[starts-with(@name, 'twitter:')]", "name");

    public static IReadOnlyList<string> JsonLd(HtmlDocument document) =>
        document.Nodes("//script[@type='application/ld+json']")
            .Select(script => script.InnerText?.Trim())
            .OfType<string>()
            .Where(json => !string.IsNullOrWhiteSpace(json))
            .ToList();

    private static Dictionary<string, string> MetaPairs(HtmlDocument document, string xpath, string keyAttribute)
    {
        var pairs = new Dictionary<string, string>();
        foreach (var node in document.Nodes(xpath))
        {
            var key = node.GetAttributeValue(keyAttribute, string.Empty);
            var content = node.GetAttributeValue("content", string.Empty);
            if (!string.IsNullOrWhiteSpace(key) && !string.IsNullOrWhiteSpace(content))
            {
                pairs[key] = content;
            }
        }

        return pairs;
    }
}
