using FluentMigrator.Runner;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using SamoBot.Migrations.Migrations;
using Testcontainers.PostgreSql;

namespace SamoBot.Tests.Postgres;

public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder().WithImage("postgres:16").Build();

    public NpgsqlDataSource DataSource { get; private set; } = null!;

    public string ConnectionString => _container.GetConnectionString();

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        var connectionString = _container.GetConnectionString();

        using var provider = new ServiceCollection()
            .AddFluentMigratorCore()
            .ConfigureRunner(runner => runner
                .AddPostgres()
                .WithGlobalConnectionString(connectionString)
                .ScanIn(typeof(RedesignFrontier).Assembly).For.Migrations())
            .BuildServiceProvider();
        provider.GetRequiredService<IMigrationRunner>().MigrateUp();

        DataSource = NpgsqlDataSource.Create(connectionString);
    }

    public async Task DisposeAsync()
    {
        await DataSource.DisposeAsync();
        await _container.DisposeAsync();
    }

    public async Task Reset()
    {
        await using var command = DataSource.CreateCommand(
            """TRUNCATE "CrawlJobs", "DiscoveredUrls", "UrlFetches", "ParsedDocuments", "Hosts", "RobotsTxt", "CrawlJobEvents" RESTART IDENTITY CASCADE""");
        await command.ExecuteNonQueryAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class PostgresDatabaseDefinition : ICollectionFixture<PostgresFixture>
{
    public const string Name = "Postgres";
}
