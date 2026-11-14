using System.Net;
using System.Net.Sockets;

namespace SamoBot.Core.Common;

/// <summary>
/// Decides whether the crawler may connect to an IP address. Pure: no I/O.
/// Private, loopback, link-local (cloud metadata), CGNAT, multicast, reserved and documentation ranges are denied.
/// IPv6 forms that carry an IPv4 address (mapped, compatible, NAT64, 6to4, Teredo) are judged by that embedded address.
/// </summary>
public static class IpAddressPolicy
{
    public static bool IsAllowed(IPAddress address) => address.AddressFamily switch
    {
        AddressFamily.InterNetwork => IsAllowedV4(address.GetAddressBytes()),
        AddressFamily.InterNetworkV6 => IsAllowedV6(address.GetAddressBytes()),
        _ => false
    };

    private static bool IsAllowedV4(byte[] bytes) => !BlockedV4.Any(range => range.Contains(bytes));

    private static bool IsAllowedV6(byte[] bytes)
    {
        if (BlockedV6.Any(range => range.Contains(bytes)))
        {
            return false;
        }

        return EmbeddedV4(bytes).All(IsAllowedV4);
    }

    /// <summary>The IPv4 addresses that an IPv6 address carries. A plain IPv6 address carries none.</summary>
    private static IEnumerable<byte[]> EmbeddedV4(byte[] bytes)
    {
        if (IPv4Compatible.Contains(bytes) || Mapped.Contains(bytes) || Siit.Contains(bytes) || Nat64.Contains(bytes))
        {
            yield return bytes[12..];
        }
        else if (SixToFour.Contains(bytes))
        {
            yield return bytes[2..6];
        }
        else if (Teredo.Contains(bytes))
        {
            yield return bytes[4..8];
            yield return [.. bytes[12..].Select(b => (byte)~b)];
        }
    }

    private static readonly Cidr[] BlockedV4 =
    [
        new([0], 8), new([10], 8), new([100, 64], 10), new([127], 8), new([169, 254], 16),
        new([172, 16], 12), new([192, 0, 0], 24), new([192, 0, 2], 24), new([192, 88, 99], 24), new([192, 168], 16),
        new([198, 18], 15), new([198, 51, 100], 24), new([203, 0, 113], 24), new([224], 4), new([240], 4)
    ];

    private static readonly Cidr[] BlockedV6 =
    [
        new(new byte[15], 120), new([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1], 128), new([0xfc], 7),
        new([0xfe, 0x80], 10), new([0xfe, 0xc0], 10), new([0xff], 8), new([0x01], 8),
        new([0x20, 0x01, 0x0d, 0xb8], 32), new([0x00, 0x64, 0xff, 0x9b, 0x00, 0x01], 48)
    ];

    private static readonly Cidr IPv4Compatible = new(new byte[12], 96);
    private static readonly Cidr Mapped = new([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff], 96);
    private static readonly Cidr Siit = new([0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0, 0], 96);
    private static readonly Cidr Nat64 = new([0, 0x64, 0xff, 0x9b, 0, 0, 0, 0, 0, 0, 0, 0], 96);
    private static readonly Cidr SixToFour = new([0x20, 0x02], 16);
    private static readonly Cidr Teredo = new([0x20, 0x01, 0, 0], 32);

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
