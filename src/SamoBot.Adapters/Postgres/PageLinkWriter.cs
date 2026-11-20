using Dapper;
using Npgsql;
using SamoBot.Core.Domain;

namespace SamoBot.Adapters.Postgres;

internal static class PageLinkWriter
{
    private const string UpsertSql = $"""
        INSERT INTO "{TableNames.Links}" ("FromUrlId", "ToUrl", "ToHost", "AnchorText", "IsInternal", "NoFollow", "FirstSeenAt")
        SELECT @FromUrlId, d.url, d.host, d.anchor, d.internal, d.nofollow, @Now
        FROM (
            SELECT DISTINCT ON (md5(t.url)) t.*
            FROM unnest(@Urls::text[], @Hosts::text[], @Anchors::text[], @Internal::boolean[], @NoFollow::boolean[])
                 WITH ORDINALITY AS t(url, host, anchor, internal, nofollow, ord)
            ORDER BY md5(t.url), t.ord
        ) d
        ORDER BY d.ord
        ON CONFLICT ("FromUrlId", md5("ToUrl")) DO UPDATE SET
            "AnchorText" = EXCLUDED."AnchorText", "IsInternal" = EXCLUDED."IsInternal", "NoFollow" = EXCLUDED."NoFollow"
        """;

    private const string DeleteStaleSql = $"""
        DELETE FROM "{TableNames.Links}"
        WHERE "FromUrlId" = @FromUrlId
          AND md5("ToUrl") <> ALL (SELECT md5(u) FROM unnest(@Urls::text[]) AS u)
        """;

    public static async Task Replace(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        int fromUrlId,
        IReadOnlyList<PageLink> links,
        DateTimeOffset now,
        CancellationToken ct)
    {
        var parameters = new
        {
            FromUrlId = fromUrlId,
            Now = now,
            Urls = links.Select(l => l.ToUrl).ToArray(),
            Hosts = links.Select(l => l.ToHost).ToArray(),
            Anchors = links.Select(l => l.AnchorText).ToArray(),
            Internal = links.Select(l => l.IsInternal).ToArray(),
            NoFollow = links.Select(l => l.NoFollow).ToArray()
        };

        if (links.Count > 0)
        {
            await connection.ExecuteAsync(new CommandDefinition(UpsertSql, parameters, transaction, cancellationToken: ct));
        }

        await connection.ExecuteAsync(new CommandDefinition(DeleteStaleSql, parameters, transaction, cancellationToken: ct));
    }
}
