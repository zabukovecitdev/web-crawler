using FluentResults;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;
using SamoBot.Core.Scope;

namespace SamoBot.Core.Jobs;

public sealed class JobTreeService(ICrawlJobRepository jobs, IJobTreeReader reader)
{
    public async Task<Result<PathTree>> Tree(int jobId, string parent, int depth, CancellationToken ct)
    {
        var job = await jobs.Find(jobId, ct);
        if (job is null)
        {
            return Result.Fail<PathTree>(new JobNotFoundError(jobId));
        }

        var problems = TreeParent.Validate(parent);
        if (problems.Count > 0)
        {
            return Result.Fail<PathTree>(problems.Select(problem => new InvalidRequestError(problem)));
        }

        var groups = await reader.Folders(jobId, parent, ct);
        var total = groups.Aggregate(FolderCounts.None, (sum, group) => sum.Plus(group.Counts));
        return Result.Ok(new PathTree(job.AllowedHosts.Count > 0 ? job.AllowedHosts[0] : string.Empty, parent, total, PathTreeBuilder.Build(parent, groups, depth)));
    }
}
