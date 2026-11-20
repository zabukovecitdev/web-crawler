using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeMarkdownConverter(string result = "# Converted\n") : IMarkdownConverter
{
    public List<(string Html, Uri PageUrl)> Calls { get; } = [];

    public string Convert(string html, Uri pageUrl)
    {
        Calls.Add((html, pageUrl));
        return result;
    }
}
