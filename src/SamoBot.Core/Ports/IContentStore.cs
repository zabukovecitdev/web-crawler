namespace SamoBot.Core.Ports;

public interface IContentStore
{
    /// <summary>Saves the content and returns the object name.</summary>
    Task<string> Save(string url, byte[] content, string? contentType, CancellationToken ct);

    Task<Stream> Load(string objectName, CancellationToken ct);
}
