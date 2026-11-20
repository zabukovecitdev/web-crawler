using System.Text;

namespace SamoBot.Core.Domain;

public sealed record FetchResponse
{
    private const int SniffLength = 4096;

    public required int StatusCode { get; init; }
    public string? ContentType { get; init; }
    public byte[]? Content { get; init; }
    public bool BodyTruncated { get; init; }
    public long ResponseTimeMs { get; init; }
    public string? RedirectLocation { get; init; }
    public string? Error { get; init; }

    public ResponseHeaders ResponseHeaders { get; init; } = ResponseHeaders.None;

    public TimeSpan? RetryAfter { get; init; }

    public bool IsBlocked { get; init; }

    public static FetchResponse BlockedAddress(long responseTimeMs = 0) => new()
    {
        StatusCode = 0,
        Error = Blocked.Reason,
        IsBlocked = true,
        ResponseTimeMs = responseTimeMs
    };

    public bool IsSuccess => StatusCode is >= 200 and <= 299;

    public bool HasContent => Content is { Length: > 0 };

    public bool IsHtml => StoredMediaTypes.IsHtml(StoredMediaTypes.MediaType(ContentType)) || LooksLikeHtml();

    public bool IsStoredType => IsHtml || StoredMediaTypes.IsStored(StoredMediaTypes.MediaType(ContentType));

    private bool LooksLikeHtml() =>
        HasContent && StoredMediaTypes.LooksLikeHtml(Encoding.UTF8.GetString(Content!, 0, Math.Min(Content!.Length, SniffLength)));
}
