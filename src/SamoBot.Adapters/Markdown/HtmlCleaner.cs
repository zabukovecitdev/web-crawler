using System.Text.RegularExpressions;
using HtmlAgilityPack;

namespace SamoBot.Adapters.Markdown;

/// <summary>Prepares page HTML for the Markdown converter: picks the main content, removes noise and makes URLs absolute.</summary>
internal static partial class HtmlCleaner
{
    private static readonly HashSet<string> NoiseTags = new(StringComparer.Ordinal)
    {
        "script", "style", "noscript", "template", "svg", "canvas", "iframe", "frame", "object", "embed", "form", "button",
        "input", "select", "textarea", "nav", "aside", "footer", "dialog", "link", "meta"
    };

    private static readonly HashSet<string> NoiseRoles = new(StringComparer.OrdinalIgnoreCase)
    {
        "navigation", "banner", "contentinfo", "complementary", "search", "dialog"
    };

    private static readonly string[] LazyImageAttributes = ["data-src", "data-lazy-src", "data-original"];
    private static readonly string[] ImageAttributesToDrop = ["srcset", "width", "height", "style", "class"];
    private static readonly string[] LinkSchemes = [Uri.UriSchemeHttp, Uri.UriSchemeHttps, Uri.UriSchemeMailto];

    public static string Clean(string html, Uri pageUrl)
    {
        var document = new HtmlDocument();
        document.LoadHtml(html);
        var baseUrl = BaseUrl(document, pageUrl);
        var root = Root(document);
        RemoveNoise(root);
        CleanImages(root, baseUrl);
        CleanLinks(root, baseUrl);
        return root.OuterHtml;
    }

    private static Uri BaseUrl(HtmlDocument document, Uri pageUrl)
    {
        var href = document.DocumentNode.SelectSingleNode("//base[@href]")?.GetAttributeValue("href", string.Empty).Trim();
        return !string.IsNullOrEmpty(href) && Uri.TryCreate(pageUrl, href, out var resolved) && IsWeb(resolved) ? resolved : pageUrl;
    }

    private static HtmlNode Root(HtmlDocument document)
    {
        var node = document.DocumentNode;
        var main = node.SelectSingleNode("//main");
        if (main is not null)
        {
            return main;
        }

        var articles = node.SelectNodes("//article");
        return articles is { Count: 1 } ? articles[0] : node.SelectSingleNode("//body") ?? node;
    }

    private static void RemoveNoise(HtmlNode root)
    {
        foreach (var node in root.Descendants().Where(IsNoise).ToList())
        {
            node.Remove();
        }
    }

    private static bool IsNoise(HtmlNode node) => node.NodeType switch
    {
        HtmlNodeType.Comment => true,
        HtmlNodeType.Element => NoiseTags.Contains(node.Name) || IsHidden(node) || NoiseRoles.Contains(node.GetAttributeValue("role", string.Empty).Trim()),
        _ => false
    };

    private static bool IsHidden(HtmlNode node) =>
        node.Attributes.Contains("hidden")
        || string.Equals(node.GetAttributeValue("aria-hidden", string.Empty).Trim(), "true", StringComparison.OrdinalIgnoreCase)
        || DisplayNone().IsMatch(node.GetAttributeValue("style", string.Empty));

    private static void CleanImages(HtmlNode root, Uri baseUrl)
    {
        foreach (var image in root.Descendants("img").ToList())
        {
            var source = ImageSource(image, baseUrl);
            if (source is null)
            {
                image.Remove();
                continue;
            }

            image.SetAttributeValue("src", source);
            foreach (var attribute in ImageAttributesToDrop)
            {
                image.Attributes.Remove(attribute);
            }
        }
    }

    private static string? ImageSource(HtmlNode image, Uri baseUrl)
    {
        var source = image.GetAttributeValue("src", string.Empty).Trim();
        if (source.Length == 0 || source.StartsWith("data:", StringComparison.OrdinalIgnoreCase))
        {
            source = LazyImageAttributes.Select(name => image.GetAttributeValue(name, string.Empty).Trim()).FirstOrDefault(value => value.Length > 0) ?? string.Empty;
        }

        return source.Length > 0 && Uri.TryCreate(baseUrl, source, out var resolved) && IsWeb(resolved) ? resolved.AbsoluteUri : null;
    }

    private static void CleanLinks(HtmlNode root, Uri baseUrl)
    {
        foreach (var anchor in root.Descendants("a").ToList())
        {
            var href = LinkTarget(anchor, baseUrl);
            if (href is null)
            {
                anchor.ParentNode.RemoveChild(anchor, keepGrandChildren: true);
                continue;
            }

            anchor.SetAttributeValue("href", href);
            if (IsEmpty(anchor))
            {
                anchor.Remove();
            }
        }
    }

    private static string? LinkTarget(HtmlNode anchor, Uri baseUrl)
    {
        var href = anchor.GetAttributeValue("href", string.Empty).Trim();
        if (href.Length == 0 || href.StartsWith('#'))
        {
            return null;
        }

        return Uri.TryCreate(baseUrl, href, out var resolved) && LinkSchemes.Contains(resolved.Scheme) ? resolved.AbsoluteUri : null;
    }

    private static bool IsEmpty(HtmlNode anchor) =>
        string.IsNullOrWhiteSpace(anchor.InnerText) && !anchor.Descendants("img").Any();

    private static bool IsWeb(Uri uri) => uri.Scheme is "http" or "https";

    [GeneratedRegex(@"display\s*:\s*none", RegexOptions.IgnoreCase)]
    private static partial Regex DisplayNone();
}
