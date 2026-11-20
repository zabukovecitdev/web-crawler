using FluentAssertions;
using System.Net;
using System.Security.Claims;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Api.Authentication;

namespace SamoBot.Tests.Api;

public class AuthSessionTests
{
    private static readonly GoogleIdentity Ann = new("sub-ann", "ann@a.example", true, "Ann A", null, null);

    private static readonly DateTimeOffset Start = new(2025, 3, 1, 9, 0, 0, TimeSpan.Zero);

    private static async Task<HttpStatusCode> MeAsync(AuthTestHost host, string cookie)
    {
        var response = await host.Client.SendAsync(AuthTestHost.Request(HttpMethod.Get, "/api/v1/me", cookie));
        return response.StatusCode;
    }

    private static EmailAllowlist Allowlist(string email) =>
        new(new TestOptionsMonitor(new AuthOptions { AllowedEmails = [email] }));

    [Fact]
    public async Task RemovingTheAccountFromTheAllowlistEndsAnExistingSession()
    {
        await using var host = await AuthTestHost.StartAsync(["ann@a.example"], []);
        host.Validator.Accept("good", Ann);
        var cookie = await host.SignInAsync("good");

        var before = await MeAsync(host, cookie);
        host.Configuration["Auth:AllowedEmailsCsv"] = "someone-else@example.com";
        host.Configuration.Reload();
        var after = await MeAsync(host, cookie);

        before.Should().Be(HttpStatusCode.OK);
        after.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task ASessionEndsTwentyFourHoursAfterSignIn()
    {
        var time = new FakeTimeProvider(Start);
        await using var host = await AuthTestHost.StartAsync(["ann@a.example"], [], time);
        host.Validator.Accept("good", Ann);
        var cookie = await host.SignInAsync("good");

        var latest = await UseSessionAsync(host, time, cookie, TimeSpan.FromHours(23));
        time.Advance(TimeSpan.FromHours(1) + TimeSpan.FromSeconds(1));
        var beyond = await MeAsync(host, latest);

        beyond.Should().Be(HttpStatusCode.Unauthorized);
    }

    private static async Task<string> UseSessionAsync(
        AuthTestHost host, FakeTimeProvider time, string cookie, TimeSpan total)
    {
        var current = cookie;
        for (var elapsed = TimeSpan.Zero; elapsed < total; elapsed += TimeSpan.FromHours(5))
        {
            time.Advance(TimeSpan.FromHours(5) < total - elapsed ? TimeSpan.FromHours(5) : total - elapsed);
            var response = await host.Client.SendAsync(AuthTestHost.Request(HttpMethod.Get, "/api/v1/me", current));
            response.StatusCode.Should().Be(HttpStatusCode.OK);
            current = AuthTestHost.CookieFrom(response) ?? current;
        }

        return current;
    }

    [Fact]
    public void ASessionWithoutAnAuthTimeClaimIsInvalid()
    {
        var identity = new ClaimsIdentity([new Claim(ClaimTypes.Email, "ann@a.example")], AuthSchemes.Cookie);

        var valid = SessionValidation.IsValid(new ClaimsPrincipal(identity), Allowlist("ann@a.example"), Start);

        valid.Should().BeFalse();
    }

    [Fact]
    public void AFreshAllowedSessionIsValid()
    {
        var principal = SessionPrincipal.Create(Ann, Start);

        SessionValidation.IsValid(principal, Allowlist("ann@a.example"), Start.AddHours(1)).Should().BeTrue();
        SessionValidation.IsValid(principal, Allowlist("bob@a.example"), Start.AddHours(1)).Should().BeFalse();
        SessionValidation.IsValid(principal, Allowlist("ann@a.example"), Start.AddHours(25)).Should().BeFalse();
    }

    private sealed class TestOptionsMonitor : IOptionsMonitor<AuthOptions>
    {
        public TestOptionsMonitor(AuthOptions value)
        {
            CurrentValue = value;
        }

        public AuthOptions CurrentValue { get; }

        public AuthOptions Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<AuthOptions, string?> listener) => null;
    }
}
