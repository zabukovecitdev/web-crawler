using Npgsql;

namespace SamoBot.Migrations;

public static class DatabaseCreator
{
    public static async Task EnsureExists(string connectionString)
    {
        var target = new NpgsqlConnectionStringBuilder(connectionString);
        var databaseName = target.Database ?? string.Empty;
        target.Database = "postgres";

        await using var connection = new NpgsqlConnection(target.ConnectionString);
        await connection.OpenAsync();

        if (!await Exists(connection, databaseName))
        {
            await Create(connection, databaseName);
        }
    }

    private static async Task<bool> Exists(NpgsqlConnection connection, string databaseName)
    {
        await using var command = new NpgsqlCommand("SELECT 1 FROM pg_database WHERE datname = @name", connection);
        command.Parameters.AddWithValue("name", databaseName);
        return await command.ExecuteScalarAsync() is not null;
    }

    private static async Task Create(NpgsqlConnection connection, string databaseName)
    {
        var quoted = new NpgsqlCommandBuilder().QuoteIdentifier(databaseName);
        await using var command = new NpgsqlCommand(string.Join(' ', "CREATE DATABASE", quoted), connection);
        await command.ExecuteNonQueryAsync();
    }
}
