using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using SamoBot.Core.Browser;
using SamoBot.Core.Crawling;
using SamoBot.Core.Directives;
using SamoBot.Core.Indexing;
using SamoBot.Core.Jobs;
using SamoBot.Core.Parsing;
using SamoBot.Core.SiteChecks;
using SamoBot.Core.Sitemaps;

namespace SamoBot.Core;

public static class CoreServiceCollectionExtensions
{
    public static IServiceCollection AddSamoBotCore(this IServiceCollection services)
    {
        services.TryAddSingleton(TimeProvider.System);
        services.AddMemoryCache();

        return services
            .AddCrawlDirectives()
            .AddCrawling()
            .AddParsing()
            .AddIndexing()
            .AddBrowser()
            .AddJobs()
            .AddSiteChecks()
            .AddSitemaps();
    }

    private static IServiceCollection AddCrawling(this IServiceCollection services) => services
        .AddScoped<RobotsGate>()
        .AddScoped<FetcherResolver>()
        .AddScoped<FetchRecorder>()
        .AddScoped<CrawlUrlHandler>()
        .AddScoped<CrawlBatchHandler>();

    private static IServiceCollection AddParsing(this IServiceCollection services) => services
        .AddScoped<ParsePageHandler>()
        .AddScoped<ParseBatchHandler>();

    private static IServiceCollection AddIndexing(this IServiceCollection services) => services
        .AddScoped<IndexBatchHandler>();

    private static IServiceCollection AddJobs(this IServiceCollection services) => services
        .AddScoped<CrawlJobService>()
        .AddScoped<PathRuleService>()
        .AddScoped<JobTreeService>()
        .AddScoped<JobCompletionHandler>()
        .AddScoped<BrowserGuardHandler>()
        .AddScoped<ReviewGuardHandler>();

    private static IServiceCollection AddBrowser(this IServiceCollection services) => services
        .AddSingleton<BrowserAvailability>()
        .AddScoped<BrowserGate>();

    private static IServiceCollection AddSiteChecks(this IServiceCollection services) => services
        .AddScoped<StartPageResolver>()
        .AddScoped<JsDetectionService>()
        .AddScoped<SitemapPlanner>()
        .AddScoped<PlanTreeService>()
        .AddScoped<SiteCheckService>();

    private static IServiceCollection AddSitemaps(this IServiceCollection services) => services
        .AddSingleton<SitemapEnqueueSchedule>()
        .AddScoped<SitemapRefresher>()
        .AddScoped<SitemapOverviewService>()
        .AddScoped<SitemapBatchHandler>();
}
