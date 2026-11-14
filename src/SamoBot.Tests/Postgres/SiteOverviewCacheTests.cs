using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Adapters.Postgres;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Postgres;

public class SiteOverviewCacheTests
{
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(30);

    private static Func<Task<SitesOverview>> Counting(Action onLoad) => () =>
    {
        onLoad();
        return Task.FromResult(new SitesOverview { Sites = 1 });
    };

    [Fact]
    public async Task ASecondCallWithinTheTtlDoesNotLoadAndALaterOneDoes()
    {
        var time = new FakeTimeProvider();
        var cache = new SiteOverviewCache(time, Ttl);
        var loads = 0;
        var load = Counting(() => loads++);

        await cache.GetOrLoad(load);
        time.Advance(TimeSpan.FromSeconds(29));
        await cache.GetOrLoad(load);
        loads.Should().Be(1);

        time.Advance(TimeSpan.FromSeconds(2));
        await cache.GetOrLoad(load);
        loads.Should().Be(2);
    }

    [Fact]
    public async Task AZeroTtlAlwaysLoads()
    {
        var cache = SiteOverviewCache.Disabled;
        var loads = 0;
        var load = Counting(() => loads++);

        await cache.GetOrLoad(load);
        await cache.GetOrLoad(load);

        loads.Should().Be(2);
    }
}
