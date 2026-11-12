using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IRobotsStore
{
    Task<RobotsRules?> Get(string host, CancellationToken ct);

    Task Save(RobotsRules rules, CancellationToken ct);
}
