using FluentAssertions;
using Microsoft.Extensions.Options;
using SamoBot.Api.Authentication;

namespace SamoBot.Tests.Api;

public class AuthAllowlistTests
{
    private static EmailAllowlist Build(AuthOptions options) => new(Options.Create(options));

    private static GoogleIdentity Id(string email, bool verified = true, string? hd = null) =>
        new("sub-1", email, verified, "Ann", null, hd);

    [Fact]
    public void EmptyListsDenyEverybody()
    {
        var list = Build(new AuthOptions());

        list.IsEmpty.Should().BeTrue();
        list.IsAllowed(Id("ann@a.example")).Should().BeFalse();
    }

    [Fact]
    public void AllowsAListedEmailIgnoringCase()
    {
        var list = Build(new AuthOptions { AllowedEmails = [" Ann@A.example "] });

        list.IsAllowed(Id("ANN@a.example")).Should().BeTrue();
        list.IsAllowed(Id("bob@a.example")).Should().BeFalse();
    }

    [Fact]
    public void AllowsAListedEmailDomain()
    {
        var list = Build(new AuthOptions { AllowedDomains = ["@a.example"] });

        list.IsAllowed(Id("bob@a.example")).Should().BeTrue();
        list.IsAllowed(Id("bob@evil-a.example")).Should().BeFalse();
    }

    [Fact]
    public void AllowsAListedHostedDomainClaim()
    {
        var list = Build(new AuthOptions { AllowedDomains = ["corp.example"] });

        list.IsAllowed(Id("bob@gmail.com", hd: "corp.example")).Should().BeTrue();
    }

    [Fact]
    public void RejectsAnUnverifiedEmailEvenWhenListed()
    {
        var list = Build(new AuthOptions { AllowedEmails = ["ann@a.example"] });

        list.IsAllowed(Id("ann@a.example", verified: false)).Should().BeFalse();
    }

    [Fact]
    public void ReadsCommaSeparatedValues()
    {
        var list = Build(new AuthOptions { AllowedEmailsCsv = "a@x.example, b@x.example", AllowedDomainsCsv = "y.example" });

        list.IsAllowed(Id("b@x.example")).Should().BeTrue();
        list.IsAllowed(Id("c@y.example")).Should().BeTrue();
        list.IsAllowed(Id("c@z.example")).Should().BeFalse();
    }
}
