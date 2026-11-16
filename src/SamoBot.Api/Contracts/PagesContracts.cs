namespace SamoBot.Api.Contracts;

/// <summary>A stored body read up to a byte cap. Truncated is true when the body is longer than the cap.</summary>
public sealed record StoredContent(byte[] Bytes, bool Truncated, long TotalBytes)
{
    public const int MaxBytes = 2 * 1024 * 1024;

    public const string TruncatedHeader = "X-Content-Truncated";

    /// <summary>The size of the whole stored body in bytes, so that a reader can check that it has all of it.</summary>
    public const string StoredBytesHeader = "X-Content-Stored-Bytes";

    public static async Task<StoredContent> Read(Stream stream, int maxBytes, CancellationToken ct)
    {
        var buffer = new byte[maxBytes + 1];
        var filled = 0;
        while (filled < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(filled), ct);
            if (read == 0)
            {
                break;
            }

            filled += read;
        }

        var total = stream.CanSeek ? stream.Length : filled;
        return filled > maxBytes ? new StoredContent(buffer[..maxBytes], true, total) : new StoredContent(buffer[..filled], false, total);
    }
}
