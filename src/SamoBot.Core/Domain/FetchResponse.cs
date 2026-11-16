using System.Text;

namespace SamoBot.Core.Domain;

/// <summary>The result of one fetch. A status code of 0 means a network error. See <see cref="Error"/>.</summary>
public sealed record FetchResponse
{
    private const int SniffLength = 4096;

    public required int StatusCode { get; init; }
    public string? ContentType { get; init; }
    public byte[]? Content { get; init; }
    public long ResponseTimeMs { get; init; }
    public string? RedirectLocation { get; init; }
    public string? Error { get; init; }

    /// <summary>The response headers the directive sources asked for (see <see cref="Directives.ResponseHeaderSelection"/>).</summary>
    public ResponseHeaders ResponseHeaders { get; init; } = ResponseHeaders.None;

    /// <summary>The wait that a Retry-After header asked for (delta seconds, or the date minus now). Null without the header.</summary>
    public TimeSpan? RetryAfter { get; init; }

    /// <summary>True when the fetch did not run because the host resolves only to disallowed addresses.</summary>
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

    /// <summary>True when the media type is HTML. A missing or wrong media type falls back to a look at the first bytes.</summary>
    public bool IsHtml => StoredMediaTypes.IsHtml(StoredMediaTypes.MediaType(ContentType)) || LooksLikeHtml();

    /// <summary>True when the crawler keeps the body: HTML (see <see cref="IsHtml"/>) or another <see cref="StoredMediaTypes"/> type.</summary>
    public bool IsStoredType => IsHtml || StoredMediaTypes.IsStored(StoredMediaTypes.MediaType(ContentType));

    private bool LooksLikeHtml() =>
        HasContent && StoredMediaTypes.LooksLikeHtml(Encoding.UTF8.GetString(Content!, 0, Math.Min(Content!.Length, SniffLength)));
}
