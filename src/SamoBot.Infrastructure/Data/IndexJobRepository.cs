using System.Data;
using Dapper;
using Samobot.Infrastructure.Enums;
using SamoBot.Infrastructure.Constants;
using SamoBot.Infrastructure.Data.Abstractions;
using SamoBot.Infrastructure.Extensions;
using SqlKata.Execution;

namespace SamoBot.Infrastructure.Data;

public class IndexJobRepository(QueryFactory queryFactory) : IIndexJobRepository
{
    public async Task<IndexJobEntity?> GetById(int id, CancellationToken cancellationToken = default)
    {
        return await queryFactory.Query(TableNames.Database.IndexJobs)
            .Where(nameof(IndexJobEntity.Id), id)
            .FirstOrDefaultAsync<IndexJobEntity>(cancellationToken: cancellationToken);
    }

    public async Task<IEnumerable<IndexJobEntity>> GetByDocumentId(int documentId, CancellationToken cancellationToken = default)
    {
        return await queryFactory.Query(TableNames.Database.IndexJobs)
            .Where(nameof(IndexJobEntity.DocumentId), documentId)
            .GetAsync<IndexJobEntity>(cancellationToken: cancellationToken);
    }

    public async Task<int> Create(int documentId, IndexJobOperation operation, CancellationToken cancellationToken = default)
    {
        return await queryFactory.Query(TableNames.Database.IndexJobs)
            .InsertGetIdAsync<int>(new
            {
                DocumentId = documentId,
                Operation = operation.AsString(),
                Status = IndexJobStatus.Pending.AsString(),
                Attempts = 0,
                ScheduledAt = DateTimeOffset.UtcNow,
                CreatedAt = DateTimeOffset.UtcNow
            }, cancellationToken: cancellationToken);
    }

    public async Task CreateBatch(IEnumerable<(int DocumentId, IndexJobOperation Operation)> jobs, CancellationToken cancellationToken = default)
    {
        var jobList = jobs.ToList();
        if (jobList.Count == 0)
            return;

        var now = DateTimeOffset.UtcNow;
        var pendingStatus = IndexJobStatus.Pending.AsString();
        var data = jobList.Select(j => new object[]
        {
            j.DocumentId,
            j.Operation.AsString(),
            pendingStatus,
            0,
            now,
            now
        });

        await queryFactory.Query(TableNames.Database.IndexJobs)
            .InsertAsync(
                new[] { "DocumentId", "Operation", "Status", "Attempts", "ScheduledAt", "CreatedAt" },
                data,
                cancellationToken: cancellationToken);
    }

    public async Task<IEnumerable<IndexJobEntity>> ClaimBatch(int limit, DateTimeOffset staleBefore, IDbTransaction transaction, CancellationToken cancellationToken = default)
    {
        if (transaction?.Connection == null)
        {
            throw new InvalidOperationException("Transaction and connection must be provided for FOR UPDATE SKIP LOCKED");
        }

        var pendingStatus = IndexJobStatus.Pending.AsString();
        var inProgressStatus = IndexJobStatus.InProgress.AsString();

        var query = queryFactory.Query(TableNames.Database.IndexJobs)
            .Where(q => q
                .Where(nameof(IndexJobEntity.Status), pendingStatus)
                .OrWhere(q2 => q2
                    .Where(nameof(IndexJobEntity.Status), inProgressStatus)
                    .Where(nameof(IndexJobEntity.ScheduledAt), "<", staleBefore)))
            .Where(nameof(IndexJobEntity.ScheduledAt), "<=", DateTimeOffset.UtcNow)
            .OrderBy(nameof(IndexJobEntity.ScheduledAt))
            .Limit(limit)
            .ForUpdateSkipLocked();

        var sqlResult = queryFactory.Compiler.Compile(query);
        var command = new CommandDefinition(sqlResult.Sql, sqlResult.NamedBindings, transaction, cancellationToken: cancellationToken);
        var entities = (await transaction.Connection.QueryAsync<IndexJobEntity>(command)).ToList();

        if (entities.Count == 0)
            return entities;

        var ids = entities.Select(e => e.Id).ToArray();
        const string updateSql = """
            UPDATE "IndexJobs"
            SET "Status" = @Status, "ScheduledAt" = @Now
            WHERE "Id" = ANY(@Ids)
            """;
        await transaction.Connection.ExecuteAsync(
            new CommandDefinition(updateSql, new { Status = inProgressStatus, Now = DateTimeOffset.UtcNow, Ids = ids }, transaction, cancellationToken: cancellationToken));

        return entities;
    }

    public async Task MarkCompleted(int jobId, CancellationToken cancellationToken = default)
    {
        await queryFactory.Query(TableNames.Database.IndexJobs)
            .Where(nameof(IndexJobEntity.Id), jobId)
            .UpdateAsync(new
            {
                Status = IndexJobStatus.Completed.AsString(),
                ProcessedAt = DateTimeOffset.UtcNow
            }, cancellationToken: cancellationToken);
    }

    public async Task MarkFailed(int jobId, string error, int maxAttempts, CancellationToken cancellationToken = default)
    {
        var job = await GetById(jobId, cancellationToken);
        if (job == null)
            return;

        var newAttempts = job.Attempts + 1;
        var newStatus = newAttempts >= maxAttempts
            ? IndexJobStatus.Failed.AsString()
            : IndexJobStatus.Pending.AsString();

        await queryFactory.Query(TableNames.Database.IndexJobs)
            .Where(nameof(IndexJobEntity.Id), jobId)
            .UpdateAsync(new
            {
                Status = newStatus,
                Attempts = newAttempts,
                LastError = error,
                ProcessedAt = newAttempts >= maxAttempts ? DateTimeOffset.UtcNow : (DateTimeOffset?)null
            }, cancellationToken: cancellationToken);
    }

    public async Task<bool> HasPendingJob(int documentId, IndexJobOperation operation, CancellationToken cancellationToken = default)
    {
        var count = await queryFactory.Query(TableNames.Database.IndexJobs)
            .Where(nameof(IndexJobEntity.DocumentId), documentId)
            .Where(nameof(IndexJobEntity.Operation), operation.AsString())
            .Where(q => q
                .Where(nameof(IndexJobEntity.Status), IndexJobStatus.Pending.AsString())
                .OrWhere(nameof(IndexJobEntity.Status), IndexJobStatus.InProgress.AsString()))
            .CountAsync<int>(cancellationToken: cancellationToken);

        return count > 0;
    }
}
