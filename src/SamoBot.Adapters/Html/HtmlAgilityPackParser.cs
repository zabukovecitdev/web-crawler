using System.Text;
using HtmlAgilityPack;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Html;

/// <summary>Parses HTML with HtmlAgilityPack. The work of each part of the page is in its own extractor.</summary>
public sealed class HtmlAgilityPackParser : IHtmlParser
{
    public ParsedPage Parse(Stream html)
    {
        var document = new HtmlDocument();
        document.Load(html, Encoding.UTF8);

        return new ParsedPage
        {
            Title = PageMetadata.Title(document),
            Description = PageMetadata.Description(document),
            Keywords = PageMetadata.Keywords(document),
            Author = PageMetadata.Author(document),
            Language = PageMetadata.Language(document),
            Canonical = PageMetadata.Canonical(document),
            Headings = PageText.Headings(document),
            BodyText = PageText.BodyText(document),
            Links = PageReferences.Links(document),
            Images = PageReferences.Images(document),
            RobotsDirectives = StructuredData.Robots(document),
            OpenGraphData = StructuredData.OpenGraph(document),
            TwitterCardData = StructuredData.TwitterCard(document),
            JsonLdData = StructuredData.JsonLd(document)
        };
    }
}
