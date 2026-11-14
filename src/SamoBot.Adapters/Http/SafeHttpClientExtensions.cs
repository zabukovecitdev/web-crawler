using System.Net;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace SamoBot.Adapters.Http;

public static class SafeHttpClientExtensions
{
    /// <summary>Registers the pieces of the connect-time address check (options, DNS, <see cref="SafeConnect"/>).</summary>
    public static IServiceCollection AddSafeNetwork(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<CrawlerNetworkOptions>(configuration.GetSection(CrawlerNetworkOptions.SectionName));
        services.AddSingleton<IDnsResolver, SystemDnsResolver>();
        services.AddSingleton<ISocketConnector, TcpSocketConnector>();
        services.AddSingleton<SafeResolver>();
        return services.AddSingleton<SafeConnect>();
    }

    /// <summary>
    /// The one way to add an HTTP client that fetches from the open web (fetcher, robots, sitemaps).
    /// Every connection of the client, redirect hops included, goes through <see cref="SafeConnect"/>.
    /// Call <see cref="AddSafeNetwork"/> once before.
    /// </summary>
    public static IHttpClientBuilder AddSafeHttpClient<TClient>(this IServiceCollection services, TimeSpan timeout, bool followRedirects)
        where TClient : class =>
        services
            .AddHttpClient<TClient>(client =>
            {
                client.Timeout = timeout;
                client.DefaultRequestHeaders.UserAgent.ParseAdd(HttpFetcher.UserAgent);
            })
            .ConfigurePrimaryHttpMessageHandler(provider => Handler(provider, followRedirects));

    private static SocketsHttpHandler Handler(IServiceProvider provider, bool followRedirects) => new()
    {
        AllowAutoRedirect = followRedirects,
        MaxAutomaticRedirections = 5,
        AutomaticDecompression = DecompressionMethods.All,
        UseProxy = provider.GetRequiredService<IOptions<CrawlerNetworkOptions>>().Value.UseProxy,
        ConnectCallback = provider.GetRequiredService<SafeConnect>().ConnectAsync
    };
}
