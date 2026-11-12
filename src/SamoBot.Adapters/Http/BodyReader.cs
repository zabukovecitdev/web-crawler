namespace SamoBot.Adapters.Http;

internal static class BodyReader
{
    /// <summary>Reads at most <paramref name="limit"/> bytes. It stops reading after the limit.</summary>
    public static async Task<byte[]> ReadLimited(Stream stream, int limit, CancellationToken ct)
    {
        using var buffer = new MemoryStream();
        var chunk = new byte[81920];
        while (buffer.Length < limit)
        {
            var wanted = (int)Math.Min(chunk.Length, limit - buffer.Length);
            var read = await stream.ReadAsync(chunk.AsMemory(0, wanted), ct);
            if (read == 0)
            {
                break;
            }

            await buffer.WriteAsync(chunk.AsMemory(0, read), ct);
        }

        return buffer.ToArray();
    }
}
