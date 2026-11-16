using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

/// <summary>Reads a stored body. <paramref name="contentType"/> is the Content-Type of the fetch (see <see cref="StoredMediaTypes"/>).</summary>
public interface IPageParser
{
    ParsedPage Parse(Stream content, string? contentType);
}
