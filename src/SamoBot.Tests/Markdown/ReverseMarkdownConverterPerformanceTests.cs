using System.Diagnostics;
using System.Globalization;
using System.Text;
using FluentAssertions;
using SamoBot.Adapters.Markdown;
using SamoBot.Api.Contracts;

namespace SamoBot.Tests.Markdown;

[Trait("Category", "Integration")]
public class ReverseMarkdownConverterPerformanceTests
{
    private const int Paragraphs = 20_000;

    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(3);

    private static string Page(int paragraphs)
    {
        var html = new StringBuilder("<html><body><main>");
        for (var i = 0; i < paragraphs; i++)
        {
            html.Append(CultureInfo.InvariantCulture, $"<p>Paragraph {i} <a href=\"/p/{i}\">a link</a> <img src=\"/i/{i}.png\" alt=\"Image\"> text.</p>");
        }

        return html.Append("</main></body></html>").ToString();
    }

    [Fact]
    public void ConvertsAPageOfAboutTwoMegabytesInUnderThreeSeconds()
    {
        var html = Page(Paragraphs);
        var pageUrl = new Uri("https://ex.com/");
        var converter = new ReverseMarkdownConverter();
        converter.Convert(Page(1_000), pageUrl);

        var fastest = TimeSpan.MaxValue;
        var markdown = string.Empty;
        for (var run = 0; run < 3; run++)
        {
            var timer = Stopwatch.StartNew();
            markdown = converter.Convert(html, pageUrl);
            fastest = timer.Elapsed < fastest ? timer.Elapsed : fastest;
        }

        html.Length.Should().BeInRange(StoredContent.MaxBytes * 8 / 10, StoredContent.MaxBytes);
        markdown.Should().Contain("[a link](https://ex.com/p/0)");
        fastest.Should().BeLessThan(Budget);
    }
}
