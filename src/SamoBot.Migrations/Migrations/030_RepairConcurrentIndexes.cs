using System.Data;
using FluentMigrator;

namespace SamoBot.Migrations.Migrations;

public sealed record ExpectedIndex(string Name, string Table, string Columns);

[Migration(20240101000030, TransactionBehavior.None, "Rebuild the concurrently built indexes that are missing, invalid or have an old definition")]
public class RepairConcurrentIndexes : Migration
{
    public static readonly IReadOnlyList<ExpectedIndex> Indexes =
    [
        new("IX_UrlFetches_DiscoveredUrlId_FetchedAt", "UrlFetches", "(\"DiscoveredUrlId\", \"FetchedAt\" DESC)"),
        new("IX_DiscoveredUrls_Host_LastCrawlAt", "DiscoveredUrls", "(\"Host\", \"LastCrawlAt\" DESC NULLS LAST, \"Id\")"),
        new("IX_DiscoveredUrls_Host_NormalizedUrl", "DiscoveredUrls", "(\"Host\", \"NormalizedUrl\")")
    ];

    private const string StateSql = """
        SELECT i.indisvalid, pg_get_indexdef(i.indexrelid)
        FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE c.relname = @name
        """;

    public override void Up() => Execute.WithConnection((connection, transaction) =>
    {
        foreach (var index in Indexes)
        {
            Repair(connection, transaction, index);
        }
    });

    public override void Down()
    {
    }

    public static bool IsHealthy(bool valid, string definition, ExpectedIndex index) =>
        valid && definition.EndsWith($" USING btree {index.Columns}", StringComparison.Ordinal);

    private static void Repair(IDbConnection connection, IDbTransaction? transaction, ExpectedIndex index)
    {
        var state = State(connection, transaction, index.Name);
        if (state is { } found && IsHealthy(found.Valid, found.Definition, index))
        {
            return;
        }

        if (state is not null)
        {
            Run(connection, transaction, $"DROP INDEX CONCURRENTLY \"{index.Name}\"");
        }

        Run(connection, transaction, $"CREATE INDEX CONCURRENTLY \"{index.Name}\" ON \"{index.Table}\" {index.Columns}");
    }

    private static (bool Valid, string Definition)? State(IDbConnection connection, IDbTransaction? transaction, string name)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = StateSql;
        var parameter = command.CreateParameter();
        parameter.ParameterName = "name";
        parameter.Value = name;
        command.Parameters.Add(parameter);
        using var reader = command.ExecuteReader();
        return reader.Read() ? (reader.GetBoolean(0), reader.GetString(1)) : null;
    }

    private static void Run(IDbConnection connection, IDbTransaction? transaction, string sql)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
