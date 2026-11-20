namespace SamoBot.Api.Contracts;

public sealed record StoredContent(byte[] Bytes, bool Truncated, long TotalBytes)
{
    public const int MaxBytes = 2 * 1024 * 1024;

    public const string TruncatedHeader = "X-Content-Truncated";

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
