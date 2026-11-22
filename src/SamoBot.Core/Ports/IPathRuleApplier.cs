using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IPathRuleApplier
{
    Task<PathRuleEffect> Apply(int crawlJobId, CancellationToken ct);
}
