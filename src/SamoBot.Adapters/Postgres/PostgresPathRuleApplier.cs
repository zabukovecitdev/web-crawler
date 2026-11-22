using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

public sealed class PostgresPathRuleApplier(NpgsqlDataSource dataSource, TimeProvider timeProvider) : IPathRuleApplier
{
    private static readonly string AllowedByRules =
        PathRuleSql.Allows("(SELECT \"PathRules\"::jsonb FROM \"CrawlJobs\" WHERE \"Id\" = @JobId)", PathRuleSql.PathOf("u.\"NormalizedUrl\""));

    private static readonly string StopSql = $"""
        UPDATE "{TableNames.DiscoveredUrls}" u
        SET "Status" = 'Disabled', "FailReason" = '{UrlFailReasons.Unselected}', "LeaseUntil" = NULL, "StatusUpdatedAt" = @Now
        WHERE u."CrawlJobId" = @JobId AND u."Status" = 'Idle' AND (u."LastCrawlAt" IS NULL OR u."FailCount" > 0)
          AND NOT {AllowedByRules}
        """;

    private static readonly string ResumeSql = $"""
        UPDATE "{TableNames.DiscoveredUrls}" u
        SET "Status" = 'Idle', "FailReason" = NULL, "FailCount" = 0, "NextCrawlAt" = @Now, "StatusUpdatedAt" = @Now
        WHERE u."CrawlJobId" = @JobId AND u."Status" = 'Disabled' AND u."FailReason" = '{UrlFailReasons.Unselected}'
          AND {AllowedByRules}
        """;

    public async Task<PathRuleEffect> Apply(int crawlJobId, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        var parameters = new { JobId = crawlJobId, Now = timeProvider.GetUtcNow() };
        var stopped = await connection.ExecuteAsync(new CommandDefinition(StopSql, parameters, transaction, cancellationToken: ct));
        var resumed = await connection.ExecuteAsync(new CommandDefinition(ResumeSql, parameters, transaction, cancellationToken: ct));
        await transaction.CommitAsync(ct);
        return new PathRuleEffect(stopped, resumed);
    }
}
