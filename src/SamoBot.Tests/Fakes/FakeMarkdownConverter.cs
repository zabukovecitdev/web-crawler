using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

/// <summary>Returns a fixed Markdown text and records the arguments of its calls.</summary>
public sealed class FakeMarkdownConverter(string result = "# Converted\n") : IMarkdownConverter
{
    public List<(string Html, Uri PageUrl)> Calls { get; } = [];

    public string Convert(string html, Uri pageUrl)
    {
        Calls.Add((html, pageUrl));
        return result;
    }
}
