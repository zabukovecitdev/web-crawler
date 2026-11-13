using System.Net.Mime;
using Microsoft.Extensions.Options;
using Minio;
using Minio.DataModel.Args;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Storage;

/// <summary>Stores page content in MinIO. The first save creates the bucket when it does not exist.</summary>
public sealed class MinioContentStore(IMinioClient client, IOptions<MinioOptions> options) : IContentStore, IDisposable
{
    private readonly SemaphoreSlim _bucketLock = new(1, 1);
    private bool _bucketReady;

    private string Bucket => options.Value.BucketName;

    public async Task<string> Save(string url, byte[] content, string? contentType, CancellationToken ct)
    {
        await EnsureBucket(ct);

        var objectName = EnsureHtmlExtension(ObjectNameGenerator.GenerateHierarchical(url, contentType));
        using var data = new MemoryStream(content, writable: false);
        var put = new PutObjectArgs()
            .WithBucket(Bucket)
            .WithObject(objectName)
            .WithStreamData(data)
            .WithObjectSize(content.LongLength)
            .WithContentType(string.IsNullOrWhiteSpace(contentType) ? MediaTypeNames.Text.Html : contentType);

        await client.PutObjectAsync(put, ct);
        return objectName;
    }

    public async Task<Stream> Load(string objectName, CancellationToken ct)
    {
        var buffer = new MemoryStream();
        var get = new GetObjectArgs()
            .WithBucket(Bucket)
            .WithObject(objectName)
            .WithCallbackStream((stream, token) => stream.CopyToAsync(buffer, token));

        await client.GetObjectAsync(get, ct);
        buffer.Position = 0;
        return buffer;
    }

    public void Dispose() => _bucketLock.Dispose();

    private async Task EnsureBucket(CancellationToken ct)
    {
        if (_bucketReady)
        {
            return;
        }

        await _bucketLock.WaitAsync(ct);
        try
        {
            if (!await client.BucketExistsAsync(new BucketExistsArgs().WithBucket(Bucket), ct))
            {
                await client.MakeBucketAsync(new MakeBucketArgs().WithBucket(Bucket), ct);
            }

            _bucketReady = true;
        }
        finally
        {
            _bucketLock.Release();
        }
    }

    // Only HTML pages reach the store. The object name always ends with .html.
    private static string EnsureHtmlExtension(string objectName) =>
        Path.GetExtension(objectName).Equals(".html", StringComparison.OrdinalIgnoreCase)
            ? objectName
            : Path.ChangeExtension(objectName, ".html");
}
