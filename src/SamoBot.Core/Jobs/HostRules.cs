using System.Net;
using SamoBot.Core.Common;

namespace SamoBot.Core.Jobs;

/// <summary>Rejects host names and IP addresses that point at the machine, the local network or a cloud metadata service.</summary>
public static class HostRules
{
    private static readonly string[] BlockedSuffixes = [".localhost", ".local", ".internal", ".localdomain", ".home.arpa"];

    /// <summary>Returns a message when the host is not allowed, otherwise null.</summary>
    public static string? Check(string host)
    {
        var name = host.TrimEnd('.').ToLowerInvariant();
        if (name.Length == 0)
        {
            return "the URL has no host.";
        }

        if (IPAddress.TryParse(name, out var address))
        {
            return IsPublic(address) ? null : "the address is local, private or reserved.";
        }

        return IsBlockedName(name) ? "the host name is local or internal." : null;
    }

    public static bool IsPublic(IPAddress address) => IpAddressPolicy.IsAllowed(address);

    // A name without a dot is a single-label name that only a local resolver knows.
    private static bool IsBlockedName(string name) =>
        name == "localhost" || !name.Contains('.') || BlockedSuffixes.Any(suffix => name.EndsWith(suffix, StringComparison.Ordinal));
}
