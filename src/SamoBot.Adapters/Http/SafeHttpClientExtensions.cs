using System.Net;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace SamoBot.Adapters.Http;

public static class SafeHttpClientExtensions
{
    public static IServiceCollection AddSafeNetwork(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<CrawlerNetworkOptions>(configuration.GetSection(CrawlerNetworkOptions.SectionName));
        services.AddSingleton<IDnsResolver, SystemDnsResolver>();
        services.AddSingleton<ISocketConnector, TcpSocketConnector>();
        services.AddSingleton<SafeResolver>();
        return services.AddSingleton<SafeConnect>();
    }

    public static IHttpClientBuilder AddSafeHttpClient<TClient>(
        this IServiceCollection services, bool followRedirects, bool decompress = true)
        where TClient : class =>
        services
            .AddHttpClient<TClient>(client =>
            {
                client.Timeout = Timeout.InfiniteTimeSpan;
                client.DefaultRequestHeaders.UserAgent.ParseAdd(HttpFetcher.UserAgent);
            })
            .ConfigurePrimaryHttpMessageHandler(provider => Handler(provider, followRedirects, decompress));

    private static SocketsHttpHandler Handler(IServiceProvider provider, bool followRedirects, bool decompress) => new()
    {
        AllowAutoRedirect = followRedirects,
        MaxAutomaticRedirections = 5,
        AutomaticDecompression = decompress ? DecompressionMethods.All : DecompressionMethods.None,
        ConnectTimeout = RequestDeadline.Seconds(provider.GetRequiredService<IOptions<CrawlerNetworkOptions>>().Value.ConnectTimeoutSeconds),
        UseProxy = provider.GetRequiredService<IOptions<CrawlerNetworkOptions>>().Value.UseProxy,
        ConnectCallback = provider.GetRequiredService<SafeConnect>().ConnectAsync
    };
}
