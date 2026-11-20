using ReverseMarkdown;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Markdown;

public sealed class ReverseMarkdownConverter : IMarkdownConverter
{
    private static readonly Config Settings = new()
    {
        GithubFlavored = true,
        Tags = { Unknown = Config.UnknownTagsOption.Bypass },
        Links = { SmartHref = true },
        Images = { Base64Handling = Config.Base64ImageHandling.Skip },
        Formatting = { RemoveComments = true, EscapeLineStarts = true, ListBulletChar = '-' }
    };

    public string Convert(string html, Uri pageUrl)
    {
        if (string.IsNullOrWhiteSpace(html))
        {
            return string.Empty;
        }

        var markdown = new Converter(Settings).Convert(HtmlCleaner.Clean(html, pageUrl));
        return MarkdownTidy.Normalize(markdown);
    }
}
