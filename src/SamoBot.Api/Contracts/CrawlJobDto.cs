using SamoBot.Core.Common;
using SamoBot.Core.Domain;

namespace SamoBot.Api.Contracts;

public sealed record CrawlJobDto(
    int Id,
    string Status,
    string Host,
    IReadOnlyList<string> SeedUrls,
    int? MaxDepth,
    int? MaxUrls,
    bool UseJsRendering,
    DateTimeOffset CreatedAt,
    DateTimeOffset? StartedAt,
    DateTimeOffset? CompletedAt,
    JobLinkSummary Links)
{
    public static CrawlJobDto From(CrawlJob job, JobLinkSummary? links = null) => new(
        job.Id,
        job.Status.ToString(),
        HostOf(job),
        job.SeedUrls,
        job.MaxDepth,
        job.MaxUrls,
        job.UseJsRendering,
        job.CreatedAt,
        job.StartedAt,
        job.CompletedAt,
        links ?? JobLinkSummary.Empty);

    private static string HostOf(CrawlJob job)
    {
        if (job.SeedUrls.Count > 0 && UrlNormalizer.TryClean(job.SeedUrls[0], out var clean))
        {
            return clean!.Authority;
        }

        return job.AllowedHosts.Count > 0 ? job.AllowedHosts[0] : string.Empty;
    }
}
