namespace SamoBot.Core.Common;

/// <summary>A host resolved only to addresses that the crawler may not connect to (see <see cref="IpAddressPolicy"/>).</summary>
public sealed class BlockedAddressException(string host) : Exception($"No allowed address for host {host}.")
{
    public string Host { get; } = host;

    /// <summary>True when this exception, or one of its inner exceptions, is a <see cref="BlockedAddressException"/>.</summary>
    public static bool IsCause(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is BlockedAddressException)
            {
                return true;
            }
        }

        return false;
    }
}
