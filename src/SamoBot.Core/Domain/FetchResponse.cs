namespace SamoBot.Core.Domain;

/// <summary>The result of one fetch. A status code of 0 means a network error. See <see cref="Error"/>.</summary>
public sealed record FetchResponse
{
    public required int StatusCode { get; init; }
    public string? ContentType { get; init; }
    public byte[]? Content { get; init; }
    public long ResponseTimeMs { get; init; }
    public string? RedirectLocation { get; init; }
    public string? Error { get; init; }

    public bool IsHtml => ContentType?.Contains("html", StringComparison.OrdinalIgnoreCase) == true;

    public bool HasContent => Content is { Length: > 0 };
}
