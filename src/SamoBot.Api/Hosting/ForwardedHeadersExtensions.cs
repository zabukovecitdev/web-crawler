using System.Net;
using Microsoft.AspNetCore.HttpOverrides;

namespace SamoBot.Api.Hosting;

/// <summary>
/// Behind a reverse proxy the peer address is the proxy's, so every client would share one rate-limit
/// bucket. Listing the proxy here makes the API read the client address from <c>X-Forwarded-For</c>.
/// Nothing is trusted by default: a header from any other peer is ignored, so it cannot be spoofed.
/// </summary>
public sealed class ForwardedHeadersSettings
{
    public const string SectionName = "ForwardedHeaders";

    /// <summary>Single proxy addresses, e.g. <c>10.0.0.5</c>.</summary>
    public string[] KnownProxies { get; set; } = [];

    /// <summary>Proxy networks in CIDR notation, e.g. <c>172.16.0.0/12</c>.</summary>
    public string[] KnownNetworks { get; set; } = [];

    public bool IsConfigured => KnownProxies.Length > 0 || KnownNetworks.Length > 0;
}

public static class ForwardedHeadersExtensions
{
    public static IServiceCollection AddSamoBotForwardedHeaders(this IServiceCollection services, IConfiguration configuration)
    {
        var settings = configuration.GetSection(ForwardedHeadersSettings.SectionName).Get<ForwardedHeadersSettings>()
                       ?? new ForwardedHeadersSettings();
        if (!settings.IsConfigured)
        {
            return services;
        }

        services.Configure<ForwardedHeadersOptions>(options =>
        {
            options.ForwardedHeaders = ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto;
            options.KnownProxies.Clear();
            options.KnownIPNetworks.Clear();
            foreach (var proxy in settings.KnownProxies)
            {
                options.KnownProxies.Add(IPAddress.Parse(proxy));
            }

            foreach (var network in settings.KnownNetworks)
            {
                options.KnownIPNetworks.Add(System.Net.IPNetwork.Parse(network));
            }
        });
        return services;
    }

    public static IApplicationBuilder UseSamoBotForwardedHeaders(this IApplicationBuilder app, IConfiguration configuration)
    {
        var settings = configuration.GetSection(ForwardedHeadersSettings.SectionName).Get<ForwardedHeadersSettings>();
        return settings is { IsConfigured: true } ? app.UseForwardedHeaders() : app;
    }
}
