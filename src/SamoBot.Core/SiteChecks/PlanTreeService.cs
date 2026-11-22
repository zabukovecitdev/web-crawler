using FluentResults;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Core.Jobs;
using SamoBot.Core.Ports;
using SamoBot.Core.Scope;

namespace SamoBot.Core.SiteChecks;

public sealed class PlanTreeService(ISitemapPlanReader reader, RobotsGate robots)
{
    public async Task<Result<PathTree>> Tree(string host, string parent, int depth, CancellationToken ct)
    {
        var problems = TreeParent.Validate(parent);
        if (problems.Count > 0)
        {
            return Result.Fail<PathTree>(problems.Select(problem => new InvalidRequestError(problem)));
        }

        var groups = await reader.Folders(host, parent, ct);
        var rules = await robots.RulesFor(host, ct);
        var nodes = PathTreeBuilder.Build(parent, groups, depth).Select(node => RobotsBlocking.Mark(node, rules)).ToList();
        return Result.Ok(new PathTree(host, parent, groups.Aggregate(FolderCounts.None, (sum, group) => sum.Plus(group.Counts)), nodes));
    }
}
