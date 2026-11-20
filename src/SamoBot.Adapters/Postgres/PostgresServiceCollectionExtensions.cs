using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Npgsql;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

public static class PostgresServiceCollectionExtensions
{
    public static IServiceCollection AddSamoBotPostgres(this IServiceCollection services, IConfiguration configuration)
    {
        var options = configuration.GetSection(DatabaseOptions.SectionName).Get<DatabaseOptions>() ?? new DatabaseOptions();
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new InvalidOperationException($"{DatabaseOptions.SectionName}:ConnectionString is not configured.");
        }

        DapperTypeHandlers.Register();
        services.AddSingleton(NpgsqlDataSource.Create(options.ConnectionString));
        services.TryAddSingleton(TimeProvider.System);
        services.AddSingleton(sp => new SiteOverviewCache(sp.GetRequiredService<TimeProvider>(), SiteOverviewCache.DefaultTtl));
        services.AddSingleton<ISiteReader, PostgresSiteReader>();
        services.AddSingleton<IPageReader, PostgresPageReader>();
        services.AddSingleton<IAnalyticsReader, PostgresAnalyticsReader>();
        return services;
    }
}
