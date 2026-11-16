using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakePageParser(ParsedPage page) : IPageParser
{
    public ParsedPage Parse(Stream content, string? contentType) => page;
}
