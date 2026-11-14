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
    public bool IsHtml => HasHtmlMediaType() || LooksLikeHtml();

    private bool HasHtmlMediaType()
    {
        if (string.IsNullOrWhiteSpace(ContentType))
        {
            return false;
        }

        var mediaType = ContentType.Split(';')[0].Trim();
        return StoredMediaTypes.IsStored(mediaType);
    }

    private bool LooksLikeHtml()
    {
        if (!HasContent)
        {
            return false;
        }

        var snippet = Encoding.UTF8.GetString(Content!, 0, Math.Min(Content!.Length, SniffLength));
        return snippet.Contains("<!doctype html", StringComparison.OrdinalIgnoreCase)
               || snippet.Contains("<html", StringComparison.OrdinalIgnoreCase);
    }
}
