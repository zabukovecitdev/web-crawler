using FluentMigrator.Runner;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using SamoBot.Migrations.Migrations;

namespace SamoBot.Tests.Postgres;

public sealed class ScratchDatabase : IAsyncDisposable
{
    private readonly NpgsqlDataSource _server;
    private readonly string _name;

    private ScratchDatabase(NpgsqlDataSource server, string connectionString, string name)
    {
        _server = server;
        _name = name;
        ConnectionString = connectionString;
        Data = NpgsqlDataSource.Create(connectionString);
    }

    public string ConnectionString { get; }

    public NpgsqlDataSource Data { get; }

    public static async Task<ScratchDatabase> Create(PostgresFixture postgres)
    {
        var name = $"migration_{Guid.NewGuid():N}";
        await using (var create = postgres.DataSource.CreateCommand($"CREATE DATABASE \"{name}\""))
        {
            await create.ExecuteNonQueryAsync();
        }

        var connectionString = new NpgsqlConnectionStringBuilder(postgres.ConnectionString) { Database = name }.ConnectionString;
        return new ScratchDatabase(postgres.DataSource, connectionString, name);
    }

    public void MigrateUp(long version) => Run(runner => runner.MigrateUp(version));

    public void MigrateDown(long version) => Run(runner => runner.MigrateDown(version));

    public async ValueTask DisposeAsync()
    {
        await Data.DisposeAsync();
        await using (var pooled = new NpgsqlConnection(ConnectionString))
        {
            NpgsqlConnection.ClearPool(pooled);
        }

        await using var drop = _server.CreateCommand($"DROP DATABASE \"{_name}\" WITH (FORCE)");
        await drop.ExecuteNonQueryAsync();
    }

    private void Run(Action<IMigrationRunner> action)
    {
        using var provider = new ServiceCollection()
            .AddFluentMigratorCore()
            .ConfigureRunner(runner => runner.AddPostgres().WithGlobalConnectionString(ConnectionString)
                .ScanIn(typeof(RedesignFrontier).Assembly).For.Migrations())
            .BuildServiceProvider();
        action(provider.GetRequiredService<IMigrationRunner>());
    }
}
