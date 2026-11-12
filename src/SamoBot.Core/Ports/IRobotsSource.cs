using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IRobotsSource
{
    Task<RobotsRules> Download(string host, CancellationToken ct);
}
