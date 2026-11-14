using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

public static class PostgresServiceCollectionExtensions
{
    /// <summary>Registers the shared data source. Each repository opens its own connection from it.</summary>
    public static IServiceCollection AddSamoBotPostgres(this IServiceCollection services, IConfiguration configuration)
    {
        var options = configuration.GetSection(DatabaseOptions.SectionName).Get<DatabaseOptions>() ?? new DatabaseOptions();
        if (string.IsNullOrWhiteSpace(options.ConnectionString))
        {
            throw new InvalidOperationException($"{DatabaseOptions.SectionName}:ConnectionString is not configured.");
        }

        DapperTypeHandlers.Register();
        services.AddSingleton(NpgsqlDataSource.Create(options.ConnectionString));
        services.AddSingleton<ISiteReader, PostgresSiteReader>();
        services.AddSingleton<IPageReader, PostgresPageReader>();
        return services;
    }
}
