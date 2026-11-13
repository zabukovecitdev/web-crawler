using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeHtmlParser(ParsedPage page) : IHtmlParser
{
    public ParsedPage Parse(Stream html) => page;
}
