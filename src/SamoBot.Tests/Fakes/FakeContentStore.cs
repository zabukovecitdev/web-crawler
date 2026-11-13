using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeContentStore : IContentStore
{
    private readonly Dictionary<string, byte[]> _objects = [];

    public List<string> SavedUrls { get; } = [];

    public void Put(string objectName, string html) => _objects[objectName] = System.Text.Encoding.UTF8.GetBytes(html);

    public Task<string> Save(string url, byte[] content, string? contentType, CancellationToken ct)
    {
        SavedUrls.Add(url);
        var name = $"object-{SavedUrls.Count}";
        _objects[name] = content;
        return Task.FromResult(name);
    }

    public Task<Stream> Load(string objectName, CancellationToken ct) =>
        Task.FromResult<Stream>(new MemoryStream(_objects[objectName]));
}
