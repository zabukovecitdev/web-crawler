using FluentAssertions;
using System.Net;
using System.Net.Http.Json;
using SamoBot.Api.Authentication;
using SamoBot.Api.Contracts;

namespace SamoBot.Tests.Api;

public class AuthOriginCheckTests
{
    private static async Task<HttpStatusCode> HubAsync(string? origin)
    {
        await using var host = await AuthTestHost.StartAsync([], []);
        var request = AuthTestHost.Request(HttpMethod.Get, "/hubs/test");
        if (origin is not null)
        {
            request.Headers.Add("Origin", origin);
        }

        return (await host.Client.SendAsync(request)).StatusCode;
    }

    [Fact]
    public async Task AForeignOriginIs403OnTheHub()
    {
        (await HubAsync("https://evil.example")).Should().Be(HttpStatusCode.Forbidden);
    }

    [Fact]
    public async Task AListedOriginPassesOnTheHub()
    {
        (await HubAsync(AuthTestHost.ListedOrigin)).Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task NoOriginPassesOnTheHub()
    {
        (await HubAsync(null)).Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task TheSameOriginPassesOnTheHub()
    {
        (await HubAsync("http://localhost")).Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task AForeignOriginIs403OnSignIn()
    {
        await using var host = await AuthTestHost.StartAsync(["ann@a.example"], []);
        host.Validator.Accept("good", new GoogleIdentity("sub-ann", "ann@a.example", true, "Ann", null, null));
        var request = AuthTestHost.Request(HttpMethod.Post, AuthRoutes.GoogleSignIn);
        request.Content = JsonContent.Create(new GoogleSignInRequest("good"));
        request.Headers.Add("Origin", "https://evil.example");

        var response = await host.Client.SendAsync(request);

        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        AuthTestHost.CookieFrom(response).Should().BeNull();
    }

    [Fact]
    public async Task OtherApiPathsDoNotCheckTheOrigin()
    {
        await using var host = await AuthTestHost.StartAsync([], []);
        var request = AuthTestHost.Request(HttpMethod.Get, "/api/v1/me");
        request.Headers.Add("Origin", "https://evil.example");
        request.Headers.Add("X-Api-Key", AuthTestHost.ApiKey);

        var response = await host.Client.SendAsync(request);

        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}
