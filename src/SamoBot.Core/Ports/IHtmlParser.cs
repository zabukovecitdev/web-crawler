using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IHtmlParser
{
    ParsedPage Parse(Stream html);
}
