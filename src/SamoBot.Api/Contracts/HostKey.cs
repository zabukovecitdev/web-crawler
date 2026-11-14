namespace SamoBot.Api.Contracts;

/// <summary>Validates and normalizes the {host} route value. Host keys are <see cref="Uri.Authority"/> values, so they round-trip.</summary>
public static class HostKey
{
    private const int MaxLength = 260;

    public static bool TryNormalize(string? value, out string host)
    {
        host = string.Empty;
        if (string.IsNullOrEmpty(value) || value.Length > MaxLength)
        {
            return false;
        }

        if (!Uri.TryCreate($"https://{value}/", UriKind.Absolute, out var uri) || !IsPlainAuthority(uri, value))
        {
            return false;
        }

        host = uri.Authority.ToLowerInvariant();
        return true;
    }

    private static bool IsPlainAuthority(Uri uri, string value) =>
        uri.UserInfo.Length == 0
        && uri.PathAndQuery == "/"
        && uri.Fragment.Length == 0
        && uri.HostNameType is UriHostNameType.Dns or UriHostNameType.IPv4 or UriHostNameType.IPv6
        && string.Equals(uri.Authority, value, StringComparison.OrdinalIgnoreCase);
}
