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

    private const string IndexSchema = "public";

    private const string StateSql = """
        SELECT i.indisvalid, pg_get_indexdef(i.indexrelid)
        FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
        WHERE c.relname = @name AND c.relnamespace = @schema::regnamespace AND i.indrelid = format('%I.%I', @schema, @table)::regclass
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
        var state = State(connection, transaction, index);
        if (state is { } found && IsHealthy(found.Valid, found.Definition, index))
        {
            return;
        }

        if (state is { Valid: true })
        {
            Replace(connection, transaction, index);
            return;
        }

        if (state is not null)
        {
            Run(connection, transaction, $"DROP INDEX CONCURRENTLY {Qualified(index.Name)}");
        }

        Run(connection, transaction, CreateSql(index.Name, index));
    }

    private static void Replace(IDbConnection connection, IDbTransaction? transaction, ExpectedIndex index)
    {
        var replacement = $"{index.Name}_rebuild";
        Run(connection, transaction, $"DROP INDEX CONCURRENTLY IF EXISTS {Qualified(replacement)}");
        Run(connection, transaction, CreateSql(replacement, index));
        Run(connection, transaction, $"DROP INDEX CONCURRENTLY {Qualified(index.Name)}");
        Run(connection, transaction, $"ALTER INDEX {Qualified(replacement)} RENAME TO \"{index.Name}\"");
    }

    private static string CreateSql(string name, ExpectedIndex index) =>
        $"CREATE INDEX CONCURRENTLY \"{name}\" ON {Qualified(index.Table)} {index.Columns}";

    private static string Qualified(string name) => $"\"{IndexSchema}\".\"{name}\"";

    private static (bool Valid, string Definition)? State(IDbConnection connection, IDbTransaction? transaction, ExpectedIndex index)
    {
        using var command = Command(connection, transaction, StateSql);
        Add(command, "name", index.Name);
        Add(command, "schema", IndexSchema);
        Add(command, "table", index.Table);
        using var reader = command.ExecuteReader();
        return reader.Read() ? (reader.GetBoolean(0), reader.GetString(1)) : null;
    }

    private static void Add(IDbCommand command, string name, string value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.Value = value;
        command.Parameters.Add(parameter);
    }

    private static void Run(IDbConnection connection, IDbTransaction? transaction, string sql)
    {
        using var command = Command(connection, transaction, sql);
        command.ExecuteNonQuery();
    }

    private static IDbCommand Command(IDbConnection connection, IDbTransaction? transaction, string sql)
    {
        var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = sql;
        command.CommandTimeout = 0;
        return command;
    }
}
