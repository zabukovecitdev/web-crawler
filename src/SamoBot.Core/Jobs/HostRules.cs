using System.Net;
using System.Net.Sockets;

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

    public static bool IsPublic(IPAddress address)
    {
        if (address.IsIPv4MappedToIPv6)
        {
            address = address.MapToIPv4();
        }

        var bytes = address.GetAddressBytes();
        return address.AddressFamily switch
        {
            AddressFamily.InterNetwork => IsPublicV4(bytes),
            AddressFamily.InterNetworkV6 => IsPublicV6(bytes),
            _ => false
        };
    }

    // A name without a dot is a single-label name that only a local resolver knows.
    private static bool IsBlockedName(string name) =>
        name == "localhost" || !name.Contains('.') || BlockedSuffixes.Any(suffix => name.EndsWith(suffix, StringComparison.Ordinal));

    private static bool IsPublicV4(byte[] bytes) => !BlockedV4.Any(range => range.Contains(bytes));

    private static bool IsPublicV6(byte[] bytes)
    {
        if (BlockedV6.Any(range => range.Contains(bytes)))
        {
            return false;
        }

        // NAT64 (64:ff9b::/96) carries an IPv4 address in the last four bytes.
        return !Nat64.Contains(bytes) || IsPublicV4(bytes[12..]);
    }

    private static readonly Cidr[] BlockedV4 =
    [
        new([0], 8), new([10], 8), new([100, 64], 10), new([127], 8), new([169, 254], 16),
        new([172, 16], 12), new([192, 0, 0], 24), new([192, 168], 16), new([198, 18], 15), new([224], 3)
    ];

    private static readonly Cidr[] BlockedV6 =
    [
        new(new byte[16], 127), new([0xfe, 0x80], 10), new([0xfe, 0xc0], 10), new([0xfc], 7), new([0xff], 8)
    ];

    private static readonly Cidr Nat64 = new([0, 0x64, 0xff, 0x9b, 0, 0, 0, 0, 0, 0, 0, 0], 96);

    /// <summary>A network as the leading bytes of its address and the number of bits that count.</summary>
    private readonly record struct Cidr(byte[] Prefix, int Bits)
    {
        public bool Contains(byte[] address)
        {
            var whole = Bits / 8;
            var rest = Bits % 8;
            var mask = (byte)(0xff << (8 - rest));
            var wholeMatch = Prefix.AsSpan(0, whole).SequenceEqual(address.AsSpan(0, whole));
            return wholeMatch && (rest == 0 || (address[whole] & mask) == (Prefix[whole] & mask));
        }
    }
}
