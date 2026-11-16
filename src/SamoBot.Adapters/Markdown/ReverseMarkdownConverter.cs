using ReverseMarkdown;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Markdown;

/// <summary>HTML to Markdown in three steps: <see cref="HtmlCleaner"/> removes the noise and makes URLs absolute, ReverseMarkdown converts, <see cref="MarkdownTidy"/> normalises.</summary>
public sealed class ReverseMarkdownConverter : IMarkdownConverter
{
    private static readonly Config Settings = new()
    {
        // Flavor = MarkdownFlavor.GitHub makes 6.3.0 return the HTML unconverted. GithubFlavored gives the same pipe tables and fenced code.
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

        // A new converter for each call: the library does not document its thread safety, and this class is a singleton.
        var markdown = new Converter(Settings).Convert(HtmlCleaner.Clean(html, pageUrl));
        return MarkdownTidy.Normalize(markdown);
    }
}
