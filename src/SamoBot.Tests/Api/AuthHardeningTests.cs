using FluentAssertions;
using System.Net;
using System.Net.Http.Json;
using Microsoft.Extensions.Hosting;
using SamoBot.Api.Authentication;
using SamoBot.Api.Contracts;
using SamoBot.Api.Hosting;

namespace SamoBot.Tests.Api;

public class AuthHardeningTests
{
    private static readonly GoogleIdentity Ann = new("sub-ann", "ann@a.example", true, "Ann A", null, null);

    private static async Task<string> SetCookieAsync(string environment, string? allowInsecure)
    {
        var settings = new Dictionary<string, string?> { ["DashboardApi:Key"] = new string('k', 64) };
        if (allowInsecure is not null)
        {
            settings["Auth:AllowInsecureCookie"] = allowInsecure;
        }

        await using var host = await AuthTestHost.StartAsync(["ann@a.example"], [], null, environment, settings);
        host.Validator.Accept("good", Ann);
        var response = await host.Client.PostAsJsonAsync(AuthRoutes.GoogleSignIn, new GoogleSignInRequest("good"));
        return response.Headers.GetValues("Set-Cookie").Single().ToLowerInvariant();
    }

    private static HttpRequestMessage Authorized(string url)
    {
        var request = AuthTestHost.Request(HttpMethod.Get, url);
        request.Headers.Add("X-Api-Key", AuthTestHost.ApiKey);
        return request;
    }

    [Fact]
    public async Task ProductionCookieIsSecureByDefault()
    {
        (await SetCookieAsync(Environments.Production, null)).Should().Contain("secure");
    }

    [Fact]
    public async Task ProductionCookieIsNotSecureWhenInsecureIsAllowed()
    {
        (await SetCookieAsync(Environments.Production, "true")).Should().NotContain("secure");
    }

    [Fact]
    public async Task DevelopmentCookieIsNotSecure()
    {
        (await SetCookieAsync(Environments.Development, null)).Should().NotContain("secure");
    }

    [Fact]
    public async Task TheCallAfterTheBurstIs429WithTheWaitInRetryAfter()
    {
        var settings = new Dictionary<string, string?> { ["RateLimiting:PermitsPerMinute"] = "2" };
        await using var host = await AuthTestHost.StartAsync([], [], extraSettings: settings);

        // A bucket never holds fewer than 3 tokens, so the fourth call is the first one refused.
        var statuses = new List<HttpStatusCode>();
        for (var i = 0; i < 3; i++)
        {
            statuses.Add((await host.Client.SendAsync(Authorized("/api/v1/me"))).StatusCode);
        }

        var refused = await host.Client.SendAsync(Authorized("/api/v1/me"));

        statuses.Should().OnlyContain(s => s == HttpStatusCode.OK);
        refused.StatusCode.Should().Be(HttpStatusCode.TooManyRequests);
        var wait = refused.Headers.RetryAfter!.Delta!.Value.TotalSeconds;
        wait.Should().BeGreaterThan(0).And.BeLessThanOrEqualTo(60);
    }

    [Fact]
    public async Task TheHubPathIsNeverLimitedForASignedInUser()
    {
        var settings = new Dictionary<string, string?> { ["RateLimiting:PermitsPerMinute"] = "1" };
        await using var host = await AuthTestHost.StartAsync([], [], extraSettings: settings);

        for (var i = 0; i < 5; i++)
        {
            var response = await host.Client.SendAsync(Authorized("/hubs/test"));
            response.StatusCode.Should().Be(HttpStatusCode.NoContent);
        }
    }
}
