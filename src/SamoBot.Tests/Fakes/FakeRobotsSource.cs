using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeRobotsSource(RobotsRules rules) : IRobotsSource
{
    public int Downloads { get; private set; }

    public Task<RobotsRules> Download(string host, CancellationToken ct)
    {
        Downloads++;
        return Task.FromResult(rules);
    }
}
