namespace SamoBot.Core.Domain;

/// <summary>A validated dashboard search. Create it with <see cref="TryCreate"/>; the limits keep a query cheap.</summary>
public sealed record SearchRequest(string Text, string? Host, int Limit, int Offset)
{
    public const int MaxTextLength = 200;
    public const int MaxHostLength = 255;
    public const int MaxLimit = 50;
    public const int MaxOffset = 10_000;

    public static bool TryCreate(string? text, string? host, int limit, int offset, out SearchRequest? request, out string error)
    {
        request = null;
        error = Validate(text, host, limit, offset);
        if (error.Length > 0)
        {
            return false;
        }

        var cleanHost = string.IsNullOrWhiteSpace(host) ? null : host.Trim().ToLowerInvariant();
        request = new SearchRequest(text!.Trim(), cleanHost, limit, offset);
        return true;
    }

    private static string Validate(string? text, string? host, int limit, int offset)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return "q is required.";
        }

        if (text.Trim().Length > MaxTextLength)
        {
            return $"q must be at most {MaxTextLength} characters.";
        }

        if (host is not null && (host.Length > MaxHostLength || host.Any(char.IsControl)))
        {
            return "host is invalid.";
        }

        if (limit is < 1 or > MaxLimit)
        {
            return $"limit must be between 1 and {MaxLimit}.";
        }

        return offset is < 0 or > MaxOffset ? $"offset must be between 0 and {MaxOffset}." : string.Empty;
    }
}
