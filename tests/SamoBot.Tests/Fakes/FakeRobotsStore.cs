using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeRobotsStore : IRobotsStore
{
    private readonly Dictionary<string, RobotsRules> _rows = [];

    public int GetCalls { get; private set; }
    public int SaveCalls { get; private set; }

    public void Seed(RobotsRules rules) => _rows[rules.Host] = rules;

    public Task<RobotsRules?> Get(string host, CancellationToken ct)
    {
        GetCalls++;
        return Task.FromResult(_rows.GetValueOrDefault(host));
    }

    public Task Save(RobotsRules rules, CancellationToken ct)
    {
        SaveCalls++;
        _rows[rules.Host] = rules;
        return Task.CompletedTask;
    }
}
