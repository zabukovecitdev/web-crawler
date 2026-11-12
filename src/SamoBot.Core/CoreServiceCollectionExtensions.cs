using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using SamoBot.Core.Crawling;
using SamoBot.Core.Indexing;
using SamoBot.Core.Jobs;
using SamoBot.Core.Parsing;

namespace SamoBot.Core;

public static class CoreServiceCollectionExtensions
{
    /// <summary>Registers the handlers and services of the core. The host adds the adapters that implement the ports.</summary>
    public static IServiceCollection AddSamoBotCore(this IServiceCollection services)
    {
        services.TryAddSingleton(TimeProvider.System);
        services.AddMemoryCache();

        return services
            .AddCrawling()
            .AddParsing()
            .AddIndexing()
            .AddJobs();
    }

    private static IServiceCollection AddCrawling(this IServiceCollection services) => services
        .AddScoped<RobotsGate>()
        .AddScoped<FetcherResolver>()
        .AddScoped<CrawlUrlHandler>()
        .AddScoped<CrawlBatchHandler>();

    private static IServiceCollection AddParsing(this IServiceCollection services) => services
        .AddScoped<ParsePageHandler>()
        .AddScoped<ParseBatchHandler>();

    private static IServiceCollection AddIndexing(this IServiceCollection services) => services
        .AddScoped<IndexBatchHandler>();

    private static IServiceCollection AddJobs(this IServiceCollection services) => services
        .AddScoped<CrawlJobService>()
        .AddScoped<JobCompletionHandler>();
}
