using FluentMigrator.Runner;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using SamoBot.Migrations.Migrations;
using Testcontainers.PostgreSql;

namespace SamoBot.Tests.Postgres;

/// <summary>One PostgreSQL container for all integration tests. It runs the real migrations.</summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder().WithImage("postgres:16").Build();

    public NpgsqlDataSource DataSource { get; private set; } = null!;

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

    /// <summary>Removes all rows. Call it at the start of each test.</summary>
    public async Task Reset()
    {
        await using var command = DataSource.CreateCommand(
            """TRUNCATE "CrawlJobs", "DiscoveredUrls", "UrlFetches", "ParsedDocuments", "Hosts", "RobotsTxt", "CrawlJobEvents" RESTART IDENTITY CASCADE""");
        await command.ExecuteNonQueryAsync();
    }
}

[CollectionDefinition(Name)]
public sealed class PostgresCollection : ICollectionFixture<PostgresFixture>
{
    public const string Name = "Postgres";
}
