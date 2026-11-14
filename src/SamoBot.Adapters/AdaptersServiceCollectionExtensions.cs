using Meilisearch;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Minio;
using SamoBot.Adapters.Browser;
using SamoBot.Adapters.Html;
using SamoBot.Adapters.Http;
using SamoBot.Adapters.Postgres;
using SamoBot.Adapters.Search;
using SamoBot.Adapters.Storage;
using SamoBot.Core.Crawling;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters;

public static class AdaptersServiceCollectionExtensions
{
    /// <summary>Registers every adapter. The configuration needs the sections Database, MinIO, Meilisearch and ChromeRendering.</summary>
    public static IServiceCollection AddSamoBotAdapters(this IServiceCollection services, IConfiguration configuration) => services
        .AddSamoBotPostgres(configuration)
        .AddPostgresAdapters()
        .AddFetchers(configuration)
        .AddStorage(configuration)
        .AddSamoBotSearch(configuration)
        .AddSingleton<IHtmlParser, HtmlAgilityPackParser>();

    private static IServiceCollection AddPostgresAdapters(this IServiceCollection services) => services
        .AddSingleton<IUrlFrontier, PostgresUrlFrontier>()
        .AddSingleton<IParseBacklog, PostgresParseBacklog>()
        .AddSingleton<IIndexBacklog, PostgresIndexBacklog>()
        .AddSingleton<ICrawlJobRepository, PostgresCrawlJobRepository>()
        .AddSingleton<IJobPageReader, PostgresJobPageReader>()
        .AddSingleton<IJobProgressReader, PostgresJobProgressReader>()
        .AddSingleton<IRobotsStore, PostgresRobotsStore>()
        .AddSingleton<ISitemapStore, PostgresSitemapStore>()
        .AddSingleton<ISitemapReader, PostgresSitemapReader>()
        .AddSingleton<IHostAddressCache>(provider => new MemoryHostAddressCache(ActivatorUtilities.CreateInstance<PostgresHostAddressCache>(provider)))
        .AddSingleton<ICrawlEventPublisher, PostgresCrawlEventPublisher>();

    private static IServiceCollection AddFetchers(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<ChromeRenderingOptions>(configuration.GetSection(ChromeRenderingOptions.SectionName));

        // The fetcher does not follow redirects, because the frontier handles them. The robots source follows a few.
        // Both connect through SafeConnect, so every hop is checked against the address policy.
        services.AddSafeNetwork(configuration);
        services.AddSingleton(provider => new PolitenessPolicy(
            TimeSpan.FromSeconds(provider.GetRequiredService<IOptions<CrawlerNetworkOptions>>().Value.MaxAdaptiveDelaySeconds)));
        services.AddSingleton(InFlightHoldFrom);
        services.AddSafeHttpClient<HttpFetcher>(followRedirects: false);
        services.AddSafeHttpClient<HttpRobotsSource>(followRedirects: true);

        // The sitemap source gets the raw bytes: no redirects and no decompression, so that the parser caps the decompressed size.
        services.AddSafeHttpClient<HttpSitemapSource>(followRedirects: false, decompress: false);

        services.AddTransient<IFetcher>(provider => provider.GetRequiredService<HttpFetcher>());
        services.AddTransient<ISitemapSource>(provider => provider.GetRequiredService<HttpSitemapSource>());
        services.AddTransient<IRobotsSource>(provider => provider.GetRequiredService<HttpRobotsSource>());
        services.AddSingleton<BrowserFetcher>();
        return services.AddSingleton<IFetcher>(provider => provider.GetRequiredService<BrowserFetcher>());
    }

    private static InFlightHold InFlightHoldFrom(IServiceProvider provider)
    {
        var network = provider.GetRequiredService<IOptions<CrawlerNetworkOptions>>().Value;
        var browser = provider.GetRequiredService<IOptions<ChromeRenderingOptions>>().Value;
        return InFlightHold.Calculate(
            TimeSpan.FromSeconds(network.RequestTimeoutSeconds),
            TimeSpan.FromMilliseconds(browser.TotalTimeoutMs),
            TimeSpan.FromSeconds(network.InFlightMarginSeconds),
            CrawlBatchHandler.Lease);
    }

    private static IServiceCollection AddStorage(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<MinioOptions>(configuration.GetSection(MinioOptions.SectionName));
        services.AddSingleton<IMinioClient>(provider => BuildMinioClient(provider.GetRequiredService<IOptions<MinioOptions>>().Value));
        return services.AddSingleton<IContentStore, MinioContentStore>();
    }

    /// <summary>Registers only the search index. The MCP server needs nothing else.</summary>
    public static IServiceCollection AddSamoBotSearch(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<MeilisearchOptions>(configuration.GetSection(MeilisearchOptions.SectionName));
        services.AddSingleton(provider =>
        {
            var options = provider.GetRequiredService<IOptions<MeilisearchOptions>>().Value;
            return new MeilisearchClient(options.Host, options.ApiKey);
        });

        services.AddSingleton<MeilisearchSettings>();
        services.AddSingleton<ISearchPages, MeilisearchPageSearch>();
        services.AddSingleton<MeilisearchIndex>();
        services.AddSingleton<ISearchIndex>(provider => provider.GetRequiredService<MeilisearchIndex>());
        return services.AddSingleton<ISearchQuery>(provider => provider.GetRequiredService<MeilisearchIndex>());
    }

    private static IMinioClient BuildMinioClient(MinioOptions options)
    {
        var client = new MinioClient()
            .WithEndpoint($"{options.Endpoint}:{options.Port}")
            .WithCredentials(options.AccessKey, options.SecretKey);

        if (options.UseSsl)
        {
            client = client.WithSSL();
        }

        if (!string.IsNullOrEmpty(options.Region))
        {
            client = client.WithRegion(options.Region);
        }

        return client.Build();
    }
}
