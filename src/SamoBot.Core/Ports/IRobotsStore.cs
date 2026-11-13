using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IRobotsStore
{
    Task<RobotsRules?> Find(string host, CancellationToken ct);

    Task Save(RobotsRules rules, CancellationToken ct);
}
