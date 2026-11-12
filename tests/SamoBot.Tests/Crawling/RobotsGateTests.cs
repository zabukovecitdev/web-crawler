using FluentAssertions;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Crawling;

public class RobotsGateTests
{
    private const string BlockPrivate = "User-agent: *\nDisallow: /private\n";

    private readonly FakeTimeProvider _time = new(TestData.Now);
    private readonly FakeRobotsStore _store = new();

    private RobotsGate Gate(RobotsRules fromSource, out FakeRobotsSource source)
    {
        source = new FakeRobotsSource(fromSource);
        return new RobotsGate(new MemoryCache(Options.Create(new MemoryCacheOptions())), _store, source, _time);
    }

    private RobotsRules Rules(string content, int hours = 24) =>
        new("a.example", content, _time.GetUtcNow(), _time.GetUtcNow().AddHours(hours));

    [Fact]
    public async Task IsAllowed_WhenRobotsAreNotRespected_ReturnsTrueWithoutLookup()
    {
        var gate = Gate(Rules(BlockPrivate), out var source);

        var allowed = await gate.IsAllowed(TestData.Url("https://a.example/private", respectRobots: false), default);

        allowed.Should().BeTrue();
        source.Downloads.Should().Be(0);
        _store.GetCalls.Should().Be(0);
    }

    [Fact]
    public async Task IsAllowed_DownloadsSavesAndAppliesRules()
    {
        var gate = Gate(Rules(BlockPrivate), out var source);

        (await gate.IsAllowed(TestData.Url("https://a.example/private/x"), default)).Should().BeFalse();
        (await gate.IsAllowed(TestData.Url("https://a.example/open"), default)).Should().BeTrue();

        source.Downloads.Should().Be(1);
        _store.SaveCalls.Should().Be(1);
    }

    [Fact]
    public async Task IsAllowed_UsesFreshStoredRulesWithoutDownload()
    {
        _store.Seed(Rules(BlockPrivate));
        var gate = Gate(Rules(string.Empty), out var source);

        var allowed = await gate.IsAllowed(TestData.Url("https://a.example/private"), default);

        allowed.Should().BeFalse();
        source.Downloads.Should().Be(0);
    }

    [Fact]
    public async Task IsAllowed_DownloadsAgainWhenStoredRulesExpired()
    {
        _store.Seed(Rules(BlockPrivate, hours: -1));
        var gate = Gate(Rules("User-agent: *\nDisallow:\n"), out var source);

        var allowed = await gate.IsAllowed(TestData.Url("https://a.example/private"), default);

        allowed.Should().BeTrue();
        source.Downloads.Should().Be(1);
    }
}
