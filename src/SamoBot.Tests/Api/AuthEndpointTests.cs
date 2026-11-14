using FluentAssertions;
using System.Net;
using System.Net.Http.Json;
using SamoBot.Api.Authentication;
using SamoBot.Api.Contracts;

namespace SamoBot.Tests.Api;

public class AuthEndpointTests
{
    private static readonly GoogleIdentity Ann =
        new("sub-ann", "ann@a.example", true, "Ann A", "https://img.example/ann.png", null);

    private static readonly GoogleIdentity Mallory =
        new("sub-mal", "mal@evil.example", true, "Mal", null, null);

    private static Task<AuthTestHost> StartAsync() => AuthTestHost.StartAsync(["ann@a.example"], []);

    [Fact]
    public async Task MeWithoutCredentialsIs401()
    {
        await using var host = await StartAsync();

        var response = await host.Client.GetAsync("/api/v1/me");

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task SignInSetsAnHttpOnlyCookieThatAuthenticatesMe()
    {
        await using var host = await StartAsync();
        host.Validator.Accept("good", Ann);

        var signIn = await host.Client.PostAsJsonAsync(AuthRoutes.GoogleSignIn, new GoogleSignInRequest("good"));
        var cookie = AuthTestHost.CookieFrom(signIn);
        var me = await host.Client.SendAsync(AuthTestHost.Request(HttpMethod.Get, "/api/v1/me", cookie));

        signIn.StatusCode.Should().Be(HttpStatusCode.NoContent);
        signIn.Headers.GetValues("Set-Cookie").Single().ToLowerInvariant().Should().Contain("httponly").And.Contain("samesite=lax");
        var body = await me.Content.ReadFromJsonAsync<MeResponse>();
        body.Should().Be(new MeResponse("sub-ann", "Ann A", "ann@a.example", "https://img.example/ann.png"));
    }

    [Fact]
    public async Task SignInForAnAccountOffTheAllowlistIs403WithoutLeakingTheList()
    {
        await using var host = await StartAsync();
        host.Validator.Accept("bad", Mallory);

        var response = await host.Client.PostAsJsonAsync(AuthRoutes.GoogleSignIn, new GoogleSignInRequest("bad"));

        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        (await response.Content.ReadAsStringAsync()).Should().NotContain("a.example");
        AuthTestHost.CookieFrom(response).Should().BeNull();
    }

    [Fact]
    public async Task SignInWithAnUnknownTokenIs401()
    {
        await using var host = await StartAsync();

        var response = await host.Client.PostAsJsonAsync(AuthRoutes.GoogleSignIn, new GoogleSignInRequest("nope"));

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task EmptyAllowlistDeniesEverybody()
    {
        await using var host = await AuthTestHost.StartAsync([], []);
        host.Validator.Accept("good", Ann);

        var response = await host.Client.PostAsJsonAsync(AuthRoutes.GoogleSignIn, new GoogleSignInRequest("good"));

        response.StatusCode.Should().Be(HttpStatusCode.Forbidden);
    }

    [Fact]
    public async Task ApiKeyStillAuthenticatesWithoutTheCsrfHeader()
    {
        await using var host = await StartAsync();
        var request = AuthTestHost.Request(HttpMethod.Post, "/api/v1/test/echo");
        request.Headers.Add("X-Api-Key", AuthTestHost.ApiKey);

        var response = await host.Client.SendAsync(request);

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
    }

    [Fact]
    public async Task WrongApiKeyIs401()
    {
        await using var host = await StartAsync();
        var request = AuthTestHost.Request(HttpMethod.Get, "/api/v1/me");
        request.Headers.Add("X-Api-Key", "wrong");

        var response = await host.Client.SendAsync(request);

        response.StatusCode.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task CookieSessionNeedsTheCsrfHeaderOnStateChangingCalls()
    {
        await using var host = await StartAsync();
        host.Validator.Accept("good", Ann);
        var cookie = await host.SignInAsync("good");

        var without = await host.Client.SendAsync(AuthTestHost.Request(HttpMethod.Post, "/api/v1/test/echo", cookie));
        var with = await host.Client.SendAsync(AuthTestHost.Request(HttpMethod.Post, "/api/v1/test/echo", cookie, csrf: true));
        var read = await host.Client.SendAsync(AuthTestHost.Request(HttpMethod.Get, "/api/v1/me", cookie));

        without.StatusCode.Should().Be(HttpStatusCode.Forbidden);
        with.StatusCode.Should().Be(HttpStatusCode.NoContent);
        read.StatusCode.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task LogoutClearsTheCookie()
    {
        await using var host = await StartAsync();
        host.Validator.Accept("good", Ann);
        var cookie = await host.SignInAsync("good");

        var response = await host.Client.SendAsync(AuthTestHost.Request(HttpMethod.Post, "/api/v1/auth/logout", cookie, csrf: true));

        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
        response.Headers.GetValues("Set-Cookie").Single().Should().Contain("expires=Thu, 01 Jan 1970");
    }
}
