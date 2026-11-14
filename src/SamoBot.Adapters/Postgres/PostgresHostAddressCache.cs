using System.Net;
using Dapper;
using Npgsql;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

/// <summary>
/// Stores the resolved addresses in the Hosts table. The cache key is the DNS name. A row of the host key
/// (the authority, with or without a port) belongs to the name when the part before the port is the name.
/// A name with no Hosts row is not stored. The in-memory layer still remembers it.
/// </summary>
public sealed class PostgresHostAddressCache(NpgsqlDataSource dataSource) : IHostAddressCache
{
    private const string FindSql = $"""
        SELECT "ResolvedAddresses", "ResolvedAt" FROM "{TableNames.Hosts}"
        WHERE ("Host" = @Host OR "Host" LIKE @Prefix) AND "ResolvedAt" IS NOT NULL
        ORDER BY "ResolvedAt" DESC LIMIT 1
        """;

    private const string SaveSql = $"""
        UPDATE "{TableNames.Hosts}" SET "ResolvedAddresses" = @Addresses, "ResolvedAt" = @ResolvedAt
        WHERE "Host" = @Host OR "Host" LIKE @Prefix
        """;

    private const string ClearSql = $"""
        UPDATE "{TableNames.Hosts}" SET "ResolvedAddresses" = NULL, "ResolvedAt" = NULL
        WHERE "Host" = @Host OR "Host" LIKE @Prefix
        """;

    public async Task<ResolvedHost?> Find(string host, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var row = await connection.QuerySingleOrDefaultAsync<HostAddressRow>(
            new CommandDefinition(FindSql, Keys(host), cancellationToken: ct));
        return row is null ? null : new ResolvedHost(host, Parse(row.ResolvedAddresses), row.ResolvedAt);
    }

    public async Task Save(ResolvedHost resolved, CancellationToken ct)
    {
        var addresses = string.Join(',', resolved.Addresses.Select(a => a.ToString()));
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await connection.ExecuteAsync(new CommandDefinition(
            SaveSql, new { resolved.Host, Prefix = $"{resolved.Host}:%", Addresses = addresses, resolved.ResolvedAt }, cancellationToken: ct));
    }

    public async Task Clear(string host, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await connection.ExecuteAsync(new CommandDefinition(ClearSql, Keys(host), cancellationToken: ct));
    }

    private static object Keys(string host) => new { Host = host, Prefix = $"{host}:%" };

    /// <summary>An address that does not parse is dropped. The caller then resolves again.</summary>
    internal static IReadOnlyList<IPAddress> Parse(string? text) =>
        (text ?? string.Empty)
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(part => IPAddress.TryParse(part, out var address) ? address : null)
            .OfType<IPAddress>()
            .ToList();
}

/// <summary>The cached columns of a Hosts row. Dapper fills the properties.</summary>
internal sealed record HostAddressRow
{
    public string? ResolvedAddresses { get; init; }
    public DateTimeOffset ResolvedAt { get; init; }
}
