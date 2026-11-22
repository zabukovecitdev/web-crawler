using System.Net;
using System.Text;
using HtmlAgilityPack;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Html;

public sealed class HtmlAgilityPackParser : IPageParser
{
    public ParsedPage Parse(Stream content, string? contentType)
    {
        var document = Load(content, StoredMediaTypes.MediaType(contentType));
        var bodyText = PageText.BodyText(document);
        var links = PageReferences.Links(document);

        return new ParsedPage
        {
            Title = PageMetadata.Title(document),
            Description = PageMetadata.Description(document),
            Keywords = PageMetadata.Keywords(document),
            Author = PageMetadata.Author(document),
            Language = PageMetadata.Language(document),
            Canonical = PageMetadata.Canonical(document),
            Headings = PageText.Headings(document),
            BodyText = bodyText,
            Links = links,
            Images = PageReferences.Images(document),
            BaseHref = PageMetadata.BaseHref(document),
            MetaTags = PageMetadata.MetaTags(document),
            OpenGraphData = StructuredData.OpenGraph(document),
            TwitterCardData = StructuredData.TwitterCard(document),
            JsonLdData = StructuredData.JsonLd(document),
            Shell = ShellSignals.Read(document, bodyText, links.Count)
        };
    }

    private static HtmlDocument Load(Stream content, string? mediaType)
    {
        var document = new HtmlDocument();
        if (StoredMediaTypes.IsMarkdown(mediaType))
        {
            document.LoadHtml(MarkdownPage.ToHtml(content));
        }
        else if (StoredMediaTypes.IsPlainText(mediaType))
        {
            document.LoadHtml(PlainTextHtml(content));
        }
        else
        {
            document.Load(content, Encoding.UTF8);
        }

        return document;
    }

    private static string PlainTextHtml(Stream content)
    {
        using var reader = new StreamReader(content, Encoding.UTF8);
        var text = reader.ReadToEnd();
        return StoredMediaTypes.LooksLikeHtml(text)
            ? text
            : $"<!doctype html><html><head></head><body><pre>{WebUtility.HtmlEncode(text)}</pre></body></html>";
    }
}
