using FluentAssertions;
using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SamoBot.Api.Authentication;
using SamoBot.Api.Contracts;
using SamoBot.Api.Controllers;

namespace SamoBot.Tests.Api;

public sealed class FakeGoogleTokenValidator : IGoogleTokenValidator
{
    private readonly Dictionary<string, GoogleIdentity> _tokens = new();

    public void Accept(string credential, GoogleIdentity identity) => _tokens[credential] = identity;

    public Task<GoogleIdentity?> ValidateAsync(string credential, CancellationToken cancellationToken) =>
        Task.FromResult(_tokens.GetValueOrDefault(credential));
}

/// <summary>A minimal in-memory API with only the auth wiring, the auth and me controllers and one echo endpoint.</summary>
public sealed class AuthTestHost : IAsyncDisposable
{
    public const string ApiKey = "test-key";

    private readonly WebApplication _app;

    private AuthTestHost(WebApplication app, FakeGoogleTokenValidator validator)
    {
        _app = app;
        Validator = validator;
        Client = app.GetTestClient();
    }

    public FakeGoogleTokenValidator Validator { get; }

    public HttpClient Client { get; }

    public static async Task<AuthTestHost> StartAsync(string[] emails, string[] domains)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = Environments.Development });
        builder.WebHost.UseTestServer();
        builder.Configuration.AddInMemoryCollection(Settings(emails, domains));
        builder.Services.AddSamoBotAuth(builder.Configuration, builder.Environment);
        var validator = new FakeGoogleTokenValidator();
        builder.Services.AddSingleton<IGoogleTokenValidator>(validator);
        builder.Services.AddControllers().AddApplicationPart(typeof(AuthController).Assembly);

        var app = builder.Build();
        app.UseSamoBotAuth();
        app.MapControllers();
        app.MapPost("/api/v1/test/echo", () => Results.NoContent()).RequireAuthorization();
        await app.StartAsync();
        return new AuthTestHost(app, validator);
    }

    public async Task<string> SignInAsync(string credential)
    {
        var response = await Client.PostAsJsonAsync(AuthRoutes.GoogleSignIn, new GoogleSignInRequest(credential));
        response.StatusCode.Should().Be(HttpStatusCode.NoContent);
        return CookieFrom(response)!;
    }

    public static string? CookieFrom(HttpResponseMessage response) =>
        response.Headers.TryGetValues("Set-Cookie", out var values)
            ? values.FirstOrDefault(v => v.StartsWith(AuthSchemes.CookieName + "=", StringComparison.Ordinal))?.Split(';')[0]
            : null;

    public static HttpRequestMessage Request(HttpMethod method, string url, string? cookie = null, bool csrf = false)
    {
        var request = new HttpRequestMessage(method, url);
        if (cookie is not null)
        {
            request.Headers.Add("Cookie", cookie);
        }

        if (csrf)
        {
            request.Headers.Add(CsrfHeaderMiddleware.HeaderName, CsrfHeaderMiddleware.HeaderValue);
        }

        return request;
    }

    public async ValueTask DisposeAsync()
    {
        Client.Dispose();
        await _app.DisposeAsync();
    }

    private static Dictionary<string, string?> Settings(string[] emails, string[] domains) => new()
    {
        ["DashboardApi:Key"] = ApiKey,
        ["Google:ClientId"] = "client-id",
        ["Auth:AllowedEmailsCsv"] = string.Join(',', emails),
        ["Auth:AllowedDomainsCsv"] = string.Join(',', domains)
    };
}
