using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IPageParser
{
    ParsedPage Parse(Stream content, string? contentType);
}
