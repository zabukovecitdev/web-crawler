using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Net.Http.Json;
using SamoBot.Api.Authentication;
using SamoBot.Api.Contracts;
using SamoBot.Api.Hosting;

namespace SamoBot.Tests.Api;

public class RateLimitingTests
{
    private const int Burst = 3;

    private static Task<AuthTestHost> StartAsync(Dictionary<string, string?>? settings = null) =>
        AuthTestHost.StartAsync(["ann@a.example"], [], extraSettings: settings);

    private static HttpRequestMessage From(string address, HttpMethod method, string url)
    {
        var request = new HttpRequestMessage(method, url);
        request.Headers.Add(AuthTestHost.RemoteHeader, address);
        return request;
    }

    private static HttpRequestMessage SignIn(string address)
    {
        var request = From(address, HttpMethod.Post, AuthRoutes.GoogleSignIn);
        request.Content = JsonContent.Create(new GoogleSignInRequest("unknown-token"));
        return request;
    }

    [Fact]
    public async Task SignInIsLimitedPerClientAddressWithRetryAfterAndAProblemBody()
    {
        await using var host = await StartAsync();

        for (var i = 0; i < Burst; i++)
        {
            (await host.Client.SendAsync(SignIn("203.0.113.7"))).StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        }

        var limited = await host.Client.SendAsync(SignIn("203.0.113.7"));
        var other = await host.Client.SendAsync(SignIn("203.0.113.8"));

        limited.StatusCode.Should().Be(HttpStatusCode.TooManyRequests);
        limited.Headers.RetryAfter!.Delta!.Value.TotalSeconds.Should().BeGreaterThan(0).And.BeLessThanOrEqualTo(60);
        (await limited.Content.ReadAsStringAsync()).Should().Contain("Too many requests");
        other.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task UnauthenticatedCallsToProtectedEndpointsAreCountedToo()
    {
        await using var host = await StartAsync(new() { ["RateLimiting:AnonymousPermitsPerMinute"] = "5" });

        for (var i = 0; i < Burst; i++)
        {
            (await host.Client.SendAsync(From("203.0.113.7", HttpMethod.Get, "/api/v1/me"))).StatusCode
                .Should().Be(HttpStatusCode.Unauthorized);
        }

        var limited = await host.Client.SendAsync(From("203.0.113.7", HttpMethod.Get, "/api/v1/me"));

        limited.StatusCode.Should().Be(HttpStatusCode.TooManyRequests);
    }

    [Fact]
    public async Task WrongApiKeysGetFewAttemptsAndTheRightKeyStillWorks()
    {
        await using var host = await StartAsync();

        for (var i = 0; i < Burst; i++)
        {
            var wrong = From("203.0.113.7", HttpMethod.Get, "/api/v1/me");
            wrong.Headers.Add(ApiKeyAuthenticationHandler.HeaderName, $"guess-{i}");
            (await host.Client.SendAsync(wrong)).StatusCode.Should().Be(HttpStatusCode.Unauthorized);
        }

        var blocked = From("203.0.113.7", HttpMethod.Get, "/api/v1/me");
        blocked.Headers.Add(ApiKeyAuthenticationHandler.HeaderName, "guess-last");
        var right = From("203.0.113.7", HttpMethod.Get, "/api/v1/me");
        right.Headers.Add(ApiKeyAuthenticationHandler.HeaderName, AuthTestHost.ApiKey);

        (await host.Client.SendAsync(blocked)).StatusCode.Should().Be(HttpStatusCode.TooManyRequests);
        (await host.Client.SendAsync(right)).StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task ASignedInUserIsNotSlowedDownByAnonymousTrafficFromTheSameAddress()
    {
        await using var host = await StartAsync(new() { ["RateLimiting:AnonymousPermitsPerMinute"] = "5" });
        for (var i = 0; i <= Burst; i++)
        {
            await host.Client.SendAsync(From("203.0.113.7", HttpMethod.Get, "/api/v1/me"));
        }

        var signedIn = From("203.0.113.7", HttpMethod.Get, "/api/v1/me");
        signedIn.Headers.Add(ApiKeyAuthenticationHandler.HeaderName, AuthTestHost.ApiKey);

        (await host.Client.SendAsync(signedIn)).StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Theory]
    [InlineData("UserPermitsPerMinute")]
    [InlineData("PermitsPerMinute")]
    public async Task SignedInUsersAreLimitedPerUserAtTheConfiguredRate(string key)
    {
        await using var host = await StartAsync(new() { [$"RateLimiting:{key}"] = "5" });

        var statuses = new List<HttpStatusCode>();
        for (var i = 0; i <= Burst; i++)
        {
            var request = From("203.0.113.7", HttpMethod.Get, "/api/v1/me");
            request.Headers.Add(ApiKeyAuthenticationHandler.HeaderName, AuthTestHost.ApiKey);
            statuses.Add((await host.Client.SendAsync(request)).StatusCode);
        }

        statuses.Take(Burst).Should().OnlyContain(s => s == HttpStatusCode.OK);
        statuses[^1].Should().Be(HttpStatusCode.TooManyRequests);
    }

    [Fact]
    public async Task AnAnonymousHubConnectionIsLimitedPerAddress()
    {
        await using var host = await StartAsync(new() { ["RateLimiting:AnonymousPermitsPerMinute"] = "5" });
        var statuses = new List<HttpStatusCode>();
        for (var i = 0; i <= Burst; i++)
        {
            statuses.Add((await host.Client.SendAsync(From("203.0.113.7", HttpMethod.Get, "/hubs/test"))).StatusCode);
        }

        statuses[^1].Should().Be(HttpStatusCode.TooManyRequests);
    }

    [Fact]
    public async Task ClientsBehindATrustedProxyAreLimitedSeparately()
    {
        await using var host = await StartAsync(new() { ["ForwardedHeaders:KnownProxies:0"] = "10.0.0.1" });
        static HttpRequestMessage Via(string client)
        {
            var request = SignIn("10.0.0.1");
            request.Headers.Add("X-Forwarded-For", client);
            return request;
        }

        for (var i = 0; i < Burst; i++)
        {
            await host.Client.SendAsync(Via("198.51.100.1"));
        }

        (await host.Client.SendAsync(Via("198.51.100.1"))).StatusCode.Should().Be(HttpStatusCode.TooManyRequests);
        (await host.Client.SendAsync(Via("198.51.100.2"))).StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task AProxyNetworkInCidrNotationIsTrusted()
    {
        await using var host = await StartAsync(new() { ["ForwardedHeaders:KnownNetworks:0"] = "172.16.0.0/12" });
        static HttpRequestMessage Via(string client)
        {
            var request = SignIn("172.18.0.4");
            request.Headers.Add("X-Forwarded-For", client);
            return request;
        }

        for (var i = 0; i < Burst; i++)
        {
            await host.Client.SendAsync(Via("198.51.100.1"));
        }

        (await host.Client.SendAsync(Via("198.51.100.1"))).StatusCode.Should().Be(HttpStatusCode.TooManyRequests);
        (await host.Client.SendAsync(Via("198.51.100.2"))).StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task ASpoofedForwardedForFromAnUntrustedPeerDoesNotBuyAFreshBucket()
    {
        await using var host = await StartAsync(new() { ["ForwardedHeaders:KnownProxies:0"] = "10.0.0.1" });
        static HttpRequestMessage Spoof(string claimed)
        {
            var request = SignIn("203.0.113.7");
            request.Headers.Add("X-Forwarded-For", claimed);
            return request;
        }

        for (var i = 0; i < Burst; i++)
        {
            await host.Client.SendAsync(Spoof($"198.51.100.{i}"));
        }

        (await host.Client.SendAsync(Spoof("198.51.100.99"))).StatusCode.Should().Be(HttpStatusCode.TooManyRequests);
    }

    [Fact]
    public async Task AnIpv6ClientIsLimitedAsAWhole64()
    {
        await using var host = await StartAsync();
        for (var i = 0; i < Burst; i++)
        {
            await host.Client.SendAsync(SignIn($"2001:db8:1:2::{i + 1}"));
        }

        (await host.Client.SendAsync(SignIn("2001:db8:1:2:ffff::9"))).StatusCode.Should().Be(HttpStatusCode.TooManyRequests);
        (await host.Client.SendAsync(SignIn("2001:db8:1:3::1"))).StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Theory]
    [InlineData("UserPermitsPerMinute")]
    [InlineData("AnonymousPermitsPerMinute")]
    [InlineData("SignInPermitsPerMinute")]
    [InlineData("ApiKeyFailuresPerMinute")]
    public async Task ANonPositiveLimitStopsTheStartup(string key)
    {
        var start = () => StartAsync(new() { [$"RateLimiting:{key}"] = "0" });

        (await start.Should().ThrowAsync<InvalidOperationException>()).WithMessage($"*{key}*");
    }

    [Theory]
    [InlineData(null, "7", 7, LegacyKeyUse.Used)]
    [InlineData("", "7", 7, LegacyKeyUse.Used)]
    [InlineData("9", "7", 9, LegacyKeyUse.Ignored)]
    [InlineData(" ", null, 300, LegacyKeyUse.NotSet)]
    [InlineData(null, null, 300, LegacyKeyUse.NotSet)]
    public void TheDeprecatedUserKeyCountsOnlyWhileTheNewOneIsBlank(string? userKey, string? legacyKey, int expected, LegacyKeyUse use)
    {
        var options = RateLimitOptions.From(Section(userKey, legacyKey));

        options.UserPermitsPerMinute.Should().Be(expected);
        options.LegacyUserKey.Should().Be(use);
    }

    [Fact]
    public void AnInvalidDeprecatedKeyIsReportedUnderItsOwnName()
    {
        var validate = () => RateLimitOptions.From(Section(null, "0")).Validate();

        validate.Should().Throw<InvalidOperationException>().WithMessage("RateLimiting:PermitsPerMinute (deprecated) must be at least 1.");
    }

    [Fact]
    public void ANonNumericLimitStopsTheStartup()
    {
        var read = () => RateLimitOptions.From(Section("lots", null));

        read.Should().Throw<InvalidOperationException>().WithMessage("*UserPermitsPerMinute*whole number*");
    }

    [Theory]
    [InlineData(null, "7", "Rename it")]
    [InlineData("9", "7", "Remove the deprecated key")]
    [InlineData("9", null, null)]
    public void StartupWarnsWhileTheDeprecatedKeyIsConfigured(string? userKey, string? legacyKey, string? warning)
    {
        var logs = new CapturingLoggerProvider();
        var services = new ServiceCollection()
            .AddLogging(logging => logging.AddProvider(logs))
            .AddSingleton(RateLimitOptions.From(Section(userKey, legacyKey)))
            .BuildServiceProvider();

        services.LogIfLegacyRateLimitKey();

        if (warning is null)
        {
            logs.Messages.Should().BeEmpty();
        }
        else
        {
            logs.Messages.Should().ContainSingle().Which.Should().Contain(warning);
        }
    }

    private static IConfigurationSection Section(string? userKey, string? legacyKey)
    {
        var settings = new Dictionary<string, string?>
        {
            ["RateLimiting:UserPermitsPerMinute"] = userKey,
            ["RateLimiting:PermitsPerMinute"] = legacyKey
        }.Where(setting => setting.Value is not null);
        return new ConfigurationBuilder().AddInMemoryCollection(settings).Build().GetSection(RateLimitOptions.SectionName);
    }

    private sealed class CapturingLoggerProvider : ILoggerProvider, ILogger
    {
        public List<string> Messages { get; } = [];

        public ILogger CreateLogger(string categoryName) => this;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Messages.Add(formatter(state, exception));

        public void Dispose()
        {
            Messages.Clear();
        }
    }
}
