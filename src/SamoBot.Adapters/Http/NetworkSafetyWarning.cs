using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SamoBot.Adapters.Http;

public static partial class NetworkSafetyWarning
{
    /// <summary>Call once after the host is built. It logs a warning when the address policy is switched off.</summary>
    public static void LogIfPrivateAddressesAllowed(this IServiceProvider services)
    {
        if (services.GetRequiredService<IOptions<CrawlerNetworkOptions>>().Value.AllowPrivateAddresses)
        {
            LogAllowed(services.GetRequiredService<ILoggerFactory>().CreateLogger(typeof(NetworkSafetyWarning).FullName!));
        }
    }

    [LoggerMessage(
        EventId = 10,
        Level = LogLevel.Warning,
        Message = "Crawler:AllowPrivateAddresses is on. The crawler can reach loopback, private and cloud metadata addresses. Use it for development and tests only.")]
    private static partial void LogAllowed(ILogger logger);
}
