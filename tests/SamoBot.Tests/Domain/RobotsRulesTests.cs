using FluentAssertions;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Domain;

public class RobotsRulesTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    [Fact]
    public void IsAllowed_AppliesDisallowRule()
    {
        var rules = new RobotsRules("a.example", "User-agent: *\nDisallow: /private\n", Now, Now.AddDays(1));

        rules.IsAllowed("/private/x", "SamoBot").Should().BeFalse();
        rules.IsAllowed("/public", "SamoBot").Should().BeTrue();
    }

    [Fact]
    public void CrawlDelay_IsReadFromContent()
    {
        var rules = new RobotsRules("a.example", "User-agent: *\nCrawl-delay: 3\n", Now, Now.AddDays(1));

        rules.CrawlDelay.Should().Be(TimeSpan.FromSeconds(3));
    }

    [Fact]
    public void CrawlDelay_IsNullWhenContentHasNone()
    {
        var rules = new RobotsRules("a.example", "User-agent: *\nDisallow:\n", Now, Now.AddDays(1));

        rules.CrawlDelay.Should().BeNull();
    }

    [Fact]
    public void AllowAll_AllowsEveryPathAndExpiresAfterTtl()
    {
        var rules = RobotsRules.AllowAll("a.example", Now, TimeSpan.FromHours(24));

        rules.IsAllowed("/anything", "SamoBot").Should().BeTrue();
        rules.ExpiresAt.Should().Be(Now.AddHours(24));
    }

    [Fact]
    public void DisallowAll_BlocksEveryPath()
    {
        var rules = RobotsRules.DisallowAll("a.example", Now, TimeSpan.FromHours(1));

        rules.IsAllowed("/", "SamoBot").Should().BeFalse();
        rules.IsAllowed("/anything", "SamoBot").Should().BeFalse();
    }
}
