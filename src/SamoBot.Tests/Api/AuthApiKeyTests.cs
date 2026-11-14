using FluentAssertions;
using System.Net;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using SamoBot.Api.Authentication;

namespace SamoBot.Tests.Api;

public class AuthApiKeyTests
{
    private static readonly string StrongKey = new('a', 64);

    private static ValidateOptionsResult Validate(string environment, string key)
    {
        var validator = new ApiKeyOptionsValidator(new FakeEnvironment(environment));
        return validator.Validate(null, new ApiKeyOptions { Key = key });
    }

    private static async Task<HttpStatusCode> CallAsync(Action<HttpRequestMessage> configure)
    {
        await using var host = await AuthTestHost.StartAsync([], []);
        var request = AuthTestHost.Request(HttpMethod.Get, "/api/v1/me");
        configure(request);
        var response = await host.Client.SendAsync(request);
        return response.StatusCode;
    }

    [Theory]
    [InlineData("change-me")]
    [InlineData("ChangeMe")]
    [InlineData("masterKey")]
    [InlineData("0123456789012345678901234567890")]
    public void ProductionRejectsAPlaceholderOrShortKey(string key)
    {
        var result = Validate(Environments.Production, key);

        result.Failed.Should().BeTrue();
        result.FailureMessage.Should().Contain("openssl rand -hex 32");
    }

    [Fact]
    public void ProductionAcceptsALongKey()
    {
        Validate(Environments.Production, StrongKey).Succeeded.Should().BeTrue();
    }

    [Fact]
    public void ProductionAcceptsAnEmptyKeyWhichDisablesTheApiKey()
    {
        Validate(Environments.Production, string.Empty).Succeeded.Should().BeTrue();
    }

    [Fact]
    public void DevelopmentAcceptsAPlaceholder()
    {
        Validate(Environments.Development, "change-me").Succeeded.Should().BeTrue();
    }

    [Fact]
    public async Task RightKeyIs200()
    {
        var status = await CallAsync(r => r.Headers.Add("X-Api-Key", AuthTestHost.ApiKey));

        status.Should().Be(HttpStatusCode.OK);
    }

    [Fact]
    public async Task WrongKeyOfADifferentLengthIs401()
    {
        var status = await CallAsync(r => r.Headers.Add("X-Api-Key", AuthTestHost.ApiKey + "-longer"));

        status.Should().Be(HttpStatusCode.Unauthorized);
    }

    [Fact]
    public async Task TwoHeaderValuesAre401()
    {
        var status = await CallAsync(r => r.Headers.Add("X-Api-Key", [AuthTestHost.ApiKey, AuthTestHost.ApiKey]));

        status.Should().Be(HttpStatusCode.Unauthorized);
    }

    private sealed class FakeEnvironment : IHostEnvironment
    {
        public FakeEnvironment(string name)
        {
            EnvironmentName = name;
        }

        public string EnvironmentName { get; set; }

        public string ApplicationName { get; set; } = "Test";

        public string ContentRootPath { get; set; } = string.Empty;

        public IFileProvider ContentRootFileProvider { get; set; } = new NullFileProvider();
    }
}
