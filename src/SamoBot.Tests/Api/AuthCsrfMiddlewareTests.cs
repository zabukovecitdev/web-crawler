using FluentAssertions;
using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using SamoBot.Api.Authentication;

namespace SamoBot.Tests.Api;

public class AuthCsrfMiddlewareTests
{
    private static async Task<(bool Called, int Status)> RunAsync(
        string method, string path, string? authType, string? header)
    {
        var called = false;
        var middleware = new CsrfHeaderMiddleware(_ =>
        {
            called = true;
            return Task.CompletedTask;
        });
        var context = new DefaultHttpContext();
        context.Request.Method = method;
        context.Request.Path = path;
        context.Response.Body = new MemoryStream();
        if (authType is not null)
        {
            context.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim(ClaimTypes.Name, "x")], authType));
        }

        if (header is not null)
        {
            context.Request.Headers[CsrfHeaderMiddleware.HeaderName] = header;
        }

        await middleware.InvokeAsync(context);
        return (called, context.Response.StatusCode);
    }

    [Theory]
    [InlineData("POST")]
    [InlineData("PUT")]
    [InlineData("PATCH")]
    [InlineData("DELETE")]
    public async Task BlocksCookieRequestsWithoutTheHeader(string method)
    {
        var (called, status) = await RunAsync(method, "/api/v1/jobs", AuthSchemes.Cookie, null);

        called.Should().BeFalse();
        status.Should().Be(403);
    }

    [Fact]
    public async Task BlocksAWrongHeaderValue()
    {
        var (called, _) = await RunAsync("POST", "/api/v1/jobs", AuthSchemes.Cookie, "other");

        called.Should().BeFalse();
    }

    [Fact]
    public async Task AllowsCookieRequestsWithTheHeader()
    {
        var (called, _) = await RunAsync("POST", "/api/v1/jobs", AuthSchemes.Cookie, CsrfHeaderMiddleware.HeaderValue);

        called.Should().BeTrue();
    }

    [Fact]
    public async Task AllowsSafeMethods()
    {
        var (called, _) = await RunAsync("GET", "/api/v1/jobs", AuthSchemes.Cookie, null);

        called.Should().BeTrue();
    }

    [Fact]
    public async Task AllowsApiKeyAndAnonymousRequests()
    {
        (await RunAsync("POST", "/api/v1/jobs", "ApiKey", null)).Called.Should().BeTrue();
        (await RunAsync("POST", "/api/v1/jobs", null, null)).Called.Should().BeTrue();
    }

    [Fact]
    public async Task ExemptsTheGoogleSignInEndpoint()
    {
        var (called, _) = await RunAsync("POST", AuthRoutes.GoogleSignIn, AuthSchemes.Cookie, null);

        called.Should().BeTrue();
    }
}
