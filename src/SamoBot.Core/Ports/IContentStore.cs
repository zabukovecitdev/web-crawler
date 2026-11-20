namespace SamoBot.Core.Ports;

public interface IContentStore
{
    Task<string> Save(string url, byte[] content, string? contentType, CancellationToken ct);

    Task<Stream> Load(string objectName, CancellationToken ct);
}
