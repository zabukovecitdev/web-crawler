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
    public async Task TheThirdCallInAMinuteIs429WithRetryAfter()
    {
        var settings = new Dictionary<string, string?> { [RateLimitingExtensions.PermitsKey] = "2" };
        await using var host = await AuthTestHost.StartAsync([], [], extraSettings: settings);

        var first = await host.Client.SendAsync(Authorized("/api/v1/me"));
        var second = await host.Client.SendAsync(Authorized("/api/v1/me"));
        var third = await host.Client.SendAsync(Authorized("/api/v1/me"));

        first.StatusCode.Should().Be(HttpStatusCode.OK);
        second.StatusCode.Should().Be(HttpStatusCode.OK);
        third.StatusCode.Should().Be(HttpStatusCode.TooManyRequests);
        third.Headers.GetValues("Retry-After").Should().Equal("60");
    }

    [Fact]
    public async Task TheHubPathIsNeverLimited()
    {
        var settings = new Dictionary<string, string?> { [RateLimitingExtensions.PermitsKey] = "1" };
        await using var host = await AuthTestHost.StartAsync([], [], extraSettings: settings);

        for (var i = 0; i < 5; i++)
        {
            var response = await host.Client.SendAsync(AuthTestHost.Request(HttpMethod.Get, "/hubs/test"));
            response.StatusCode.Should().Be(HttpStatusCode.NoContent);
        }
    }
}
