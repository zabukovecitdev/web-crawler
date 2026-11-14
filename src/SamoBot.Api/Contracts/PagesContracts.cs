namespace SamoBot.Api.Contracts;

/// <summary>A stored body read up to a byte cap. Truncated is true when the body is longer than the cap.</summary>
public sealed record StoredContent(byte[] Bytes, bool Truncated)
{
    public const int MaxBytes = 2 * 1024 * 1024;

    public const string TruncatedHeader = "X-Content-Truncated";

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

        return filled > maxBytes ? new StoredContent(buffer[..maxBytes], true) : new StoredContent(buffer[..filled], false);
    }
}
