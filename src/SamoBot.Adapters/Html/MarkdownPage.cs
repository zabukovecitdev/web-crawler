using System.Net;
using System.Text;
using Markdig;
using Markdig.Extensions.Yaml;
using Markdig.Syntax;
using Markdig.Syntax.Inlines;

namespace SamoBot.Adapters.Html;

internal static class MarkdownPage
{
    private static readonly MarkdownPipeline Pipeline = new MarkdownPipelineBuilder()
        .UseAdvancedExtensions()
        .UseYamlFrontMatter()
        .Build();

    public static string ToHtml(Stream content)
    {
        using var reader = new StreamReader(content, Encoding.UTF8);
        var markdown = reader.ReadToEnd();
        var document = Markdig.Markdown.Parse(markdown, Pipeline);
        var frontMatter = FrontMatter(document);

        var title = frontMatter.GetValueOrDefault("title") ?? FirstTitleHeading(document);
        var description = frontMatter.GetValueOrDefault("description") ?? frontMatter.GetValueOrDefault("summary");

        var html = new StringBuilder("<!doctype html><html><head>");
        if (!string.IsNullOrWhiteSpace(title))
        {
            html.Append("<title>").Append(WebUtility.HtmlEncode(title)).Append("</title>");
        }

        if (!string.IsNullOrWhiteSpace(description))
        {
            html.Append("<meta name=\"description\" content=\"").Append(WebUtility.HtmlEncode(description)).Append("\">");
        }

        html.Append("</head><body>").Append(document.ToHtml(Pipeline)).Append("</body></html>");
        return html.ToString();
    }

    private static Dictionary<string, string> FrontMatter(MarkdownDocument document)
    {
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (document.Descendants<YamlFrontMatterBlock>().FirstOrDefault() is not { } block)
        {
            return values;
        }

        foreach (var line in block.Lines.Lines.Take(block.Lines.Count))
        {
            var text = line.Slice.ToString();
            var colon = text.IndexOf(':');
            if (colon <= 0 || char.IsWhiteSpace(text[0]))
            {
                continue;
            }

            var value = Unquote(text[(colon + 1)..].Trim());
            if (value.Length > 0)
            {
                values.TryAdd(text[..colon].Trim(), value);
            }
        }

        return values;
    }

    private static string Unquote(string value) =>
        value.Length >= 2 && (value[0] == '"' || value[0] == '\'') && value[^1] == value[0] ? value[1..^1] : value;

    private static string? FirstTitleHeading(MarkdownDocument document) =>
        document.Descendants<HeadingBlock>().FirstOrDefault(h => h.Level == 1)?.Inline is { } inline
            ? string.Concat(inline.Descendants<LiteralInline>().Select(l => l.Content.ToString()))
            : null;
}
