namespace SamoBot.Core.Common;

public sealed class BlockedAddressException(string host) : Exception($"No allowed address for host {host}.")
{
    public string Host { get; } = host;

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
