using System.Text.RegularExpressions;
using HtmlAgilityPack;
using SamoBot.Core.Domain;

namespace SamoBot.Adapters.Html;

internal static partial class ShellSignals
{
    private const string AppContainers =
        "//*[@id='root' or @id='app' or @id='__next' or @id='___gatsby' or @id='svelte' or local-name()='app-root']";

    private const string ExecutableScripts = "//script[not(@type) or @type='' or @type='module' or contains(@type, 'javascript')]";

    private static readonly string[] InertTags = ["script", "style", "noscript", "template", "link", "meta"];

    public static PageShell Read(HtmlDocument document, string bodyText, int linkCount)
    {
        var scripts = document.Nodes(ExecutableScripts).ToList();
        return new PageShell(
            bodyText.Length,
            linkCount,
            scripts.Count,
            scripts.Where(script => !script.Attributes.Contains("src")).Sum(script => script.InnerText.Length),
            document.Nodes(AppContainers).Any(IsEmpty),
            document.Nodes("//noscript").Any(IsEnableNotice));
    }

    private static bool IsEmpty(HtmlNode container) =>
        !container.ChildNodes.Any(child => child.NodeType == HtmlNodeType.Element && !InertTags.Contains(child.Name));

    private static bool IsEnableNotice(HtmlNode noscript) => Notice().IsMatch(noscript.InnerText);

    [GeneratedRegex(@"javascript.*(enable|required|turn on|must|need|disabled)|(enable|required|turn on|must|need).*javascript", RegexOptions.IgnoreCase | RegexOptions.Singleline)]
    private static partial Regex Notice();
}
