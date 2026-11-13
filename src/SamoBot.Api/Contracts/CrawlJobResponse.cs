using System.Text.Json;
using SamoBot.Core.Common;
using SamoBot.Core.Domain;

namespace SamoBot.Api.Contracts;

/// <summary>The JSON shape of a job. It stays as the dashboard knows it: the status and the seed URLs are strings.</summary>
public sealed record CrawlJobResponse(
    int Id,
    string? OwnerUserId,
    string Status,
    string SeedUrls,
    int? MaxDepth,
    int? MaxUrls,
    bool UseJsRendering,
    bool RespectRobots,
    DateTimeOffset CreatedAt,
    DateTimeOffset UpdatedAt,
    DateTimeOffset? StartedAt,
    DateTimeOffset? CompletedAt)
{
    public static CrawlJobResponse From(CrawlJob job) => new(
        job.Id,
        job.OwnerUserId,
        job.Status.ToString(),
        JsonSerializer.Serialize(job.SeedUrls, JsonDefaults.Options),
        job.MaxDepth,
        job.MaxUrls,
        job.UseJsRendering,
        job.RespectRobots,
        job.CreatedAt,
        job.UpdatedAt,
        job.StartedAt,
        job.CompletedAt);
}
