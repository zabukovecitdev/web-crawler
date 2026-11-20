namespace SamoBot.Adapters.Http;

internal readonly record struct LimitedBody(byte[] Bytes, bool Truncated);

internal static class BodyReader
{
    public static async Task<LimitedBody> ReadLimited(Stream stream, int limit, CancellationToken ct)
    {
        using var buffer = new MemoryStream();
        var chunk = new byte[81920];
        while (buffer.Length < limit)
        {
            var wanted = (int)Math.Min(chunk.Length, limit - buffer.Length);
            var read = await stream.ReadAsync(chunk.AsMemory(0, wanted), ct);
            if (read == 0)
            {
                return new LimitedBody(buffer.ToArray(), false);
            }

            await buffer.WriteAsync(chunk.AsMemory(0, read), ct);
        }

        var more = await stream.ReadAsync(chunk.AsMemory(0, 1), ct);
        return new LimitedBody(buffer.ToArray(), more > 0);
    }
}
