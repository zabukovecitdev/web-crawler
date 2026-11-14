using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Adapters.Http;
using SamoBot.Core.Common;
using SamoBot.Core.Ports;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Http;

public class DnsCacheTests
{
    private sealed class CountingDns : IDnsResolver
    {
        public Dictionary<string, IPAddress[]> Map { get; } = [];

        public int Lookups { get; private set; }

        public Task<IReadOnlyList<IPAddress>> Resolve(string host, CancellationToken ct)
        {
            Lookups++;
            return Task.FromResult<IReadOnlyList<IPAddress>>(Map[host]);
        }
    }

    private sealed class FlakyConnector(params string[] failing) : ISocketConnector
    {
        public List<string> Attempts { get; } = [];

        public ValueTask<Stream> Connect(IPAddress address, int port, CancellationToken ct)
        {
            Attempts.Add(address.ToString());
            return failing.Contains(address.ToString())
                ? throw new SocketException((int)SocketError.ConnectionRefused)
                : ValueTask.FromResult<Stream>(new MemoryStream());
        }
    }

    private readonly FakeTimeProvider _time = new(new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero));
    private readonly CountingDns _dns = new();
    private readonly FakeHostAddressCache _cache = new();

    private SafeResolver Resolver(int minutes = 60, bool allowPrivate = false) => new(
        _dns, _cache, Options.Create(new CrawlerNetworkOptions { DnsCacheMinutes = minutes, AllowPrivateAddresses = allowPrivate }), _time);

    private static IPAddress Ip(string text) => IPAddress.Parse(text);

    private void Cached(string host, DateTimeOffset at, params string[] addresses) =>
        _cache.Entries[host] = new ResolvedHost(host, addresses.Select(Ip).ToList(), at);

    [Fact]
    public async Task ResolveAllowedAfterAFreshLookupStoresTheAddressesAndTheSecondCallSkipsDns()
    {
        _dns.Map["a.example"] = [Ip("8.8.8.8"), Ip("10.0.0.1")];
        var resolver = Resolver();

        var first = await resolver.ResolveAllowed("a.example", default);
        var second = await resolver.ResolveAllowed("a.example", default);

        first.FromCache.Should().BeFalse();
        second.FromCache.Should().BeTrue();
        second.Addresses.Should().Equal(Ip("8.8.8.8"));
        _dns.Lookups.Should().Be(1);
        _cache.Entries["a.example"].ResolvedAt.Should().Be(_time.GetUtcNow());
    }

    [Fact]
    public async Task ResolveAllowedAfterTheTtlResolvesAgain()
    {
        _dns.Map["a.example"] = [Ip("8.8.8.8")];
        Cached("a.example", _time.GetUtcNow().AddMinutes(-61), "1.1.1.1");

        var result = await Resolver().ResolveAllowed("a.example", default);

        result.Addresses.Should().Equal(Ip("8.8.8.8"));
        _dns.Lookups.Should().Be(1);
    }

    [Fact]
    public async Task ResolveAllowedRevalidatesCachedAddressesOnEveryUse()
    {
        _dns.Map["a.example"] = [Ip("8.8.8.8")];
        Cached("a.example", _time.GetUtcNow(), "10.0.0.9", "1.1.1.1");

        var result = await Resolver().ResolveAllowed("a.example", default);

        result.Addresses.Should().Equal(Ip("1.1.1.1"));
        result.FromCache.Should().BeTrue();
        _dns.Lookups.Should().Be(0);
    }

    [Fact]
    public async Task ResolveAllowedWhenTheCachedAddressesAreNowBlockedResolvesAgain()
    {
        _dns.Map["a.example"] = [Ip("8.8.8.8")];
        Cached("a.example", _time.GetUtcNow(), "169.254.169.254");

        var result = await Resolver().ResolveAllowed("a.example", default);

        result.Addresses.Should().Equal(Ip("8.8.8.8"));
        result.FromCache.Should().BeFalse();
    }

    [Fact]
    public async Task ResolveAllowedWhenDnsGivesOnlyBlockedAddressesClearsTheCacheAndThrows()
    {
        _dns.Map["rebind.example"] = [Ip("127.0.0.1")];
        Cached("rebind.example", _time.GetUtcNow().AddMinutes(-120), "8.8.8.8");

        var act = () => Resolver().ResolveAllowed("rebind.example", default);

        await act.Should().ThrowAsync<BlockedAddressException>();
        _cache.Entries.Should().NotContainKey("rebind.example");
        _cache.Clears.Should().Be(1);
    }

    [Fact]
    public async Task ResolveAllowedWithZeroMinutesNeverUsesOrStoresTheCache()
    {
        _dns.Map["a.example"] = [Ip("8.8.8.8")];
        Cached("a.example", _time.GetUtcNow(), "1.1.1.1");
        var resolver = Resolver(minutes: 0);

        await resolver.ResolveAllowed("a.example", default);
        await resolver.ResolveAllowed("a.example", default);

        _dns.Lookups.Should().Be(2);
        _cache.Saves.Should().Be(0);
    }

    [Fact]
    public async Task ResolveAllowedForAnIpLiteralTouchesNeitherDnsNorTheCache()
    {
        var result = await Resolver().ResolveAllowed("8.8.4.4", default);

        result.Addresses.Should().Equal(Ip("8.8.4.4"));
        (_dns.Lookups, _cache.Saves).Should().Be((0, 0));
    }

    [Fact]
    public async Task ConnectWhenCachedAddressesFailResolvesOnceUpdatesTheCacheAndRetries()
    {
        _dns.Map["a.example"] = [Ip("9.9.9.9")];
        Cached("a.example", _time.GetUtcNow(), "8.8.8.8");
        var connector = new FlakyConnector("8.8.8.8");
        var connect = new SafeConnect(Resolver(), connector);

        await connect.Connect(new DnsEndPoint("a.example", 443), default);

        connector.Attempts.Should().Equal("8.8.8.8", "9.9.9.9");
        _dns.Lookups.Should().Be(1);
        _cache.Entries["a.example"].Addresses.Should().Equal(Ip("9.9.9.9"));
    }

    [Fact]
    public async Task ConnectWhenFreshAddressesAlsoFailThrowsAfterOneRetryOnly()
    {
        _dns.Map["a.example"] = [Ip("9.9.9.9")];
        Cached("a.example", _time.GetUtcNow(), "8.8.8.8");
        var connector = new FlakyConnector("8.8.8.8", "9.9.9.9");
        var connect = new SafeConnect(Resolver(), connector);

        var act = async () => await connect.Connect(new DnsEndPoint("a.example", 443), default);

        await act.Should().ThrowAsync<SocketException>();
        connector.Attempts.Should().HaveCount(2);
        _dns.Lookups.Should().Be(1);
    }

    [Fact]
    public async Task ConnectWhenTheRetryResolvesOnlyBlockedAddressesClearsTheCacheAndThrowsBlocked()
    {
        _dns.Map["a.example"] = [Ip("192.168.0.7")];
        Cached("a.example", _time.GetUtcNow(), "8.8.8.8");
        var connect = new SafeConnect(Resolver(), new FlakyConnector("8.8.8.8"));

        var act = async () => await connect.Connect(new DnsEndPoint("a.example", 443), default);

        await act.Should().ThrowAsync<BlockedAddressException>();
        _cache.Entries.Should().NotContainKey("a.example");
    }

    [Fact]
    public async Task MemoryLayerAnswersRepeatedLookupsWithoutAskingTheInnerCacheAgain()
    {
        var inner = new FakeHostAddressCache();
        inner.Entries["a.example"] = new ResolvedHost("a.example", [Ip("8.8.8.8")], _time.GetUtcNow());
        var memory = new MemoryHostAddressCache(inner);

        var first = await memory.Find("a.example", default);
        inner.Entries.Clear();
        var second = await memory.Find("A.example", default);

        second.Should().BeSameAs(first);
    }

    [Fact]
    public async Task MemoryLayerWritesThroughSaveAndClear()
    {
        var inner = new FakeHostAddressCache();
        var memory = new MemoryHostAddressCache(inner);
        var resolved = new ResolvedHost("a.example", [Ip("8.8.8.8")], _time.GetUtcNow());

        await memory.Save(resolved, default);
        (await memory.Find("a.example", default)).Should().Be(resolved);
        await memory.Clear("a.example", default);

        (await memory.Find("a.example", default)).Should().BeNull();
        inner.Entries.Should().BeEmpty();
    }
}
