using FluentAssertions;
using SamoBot.Core.Common;

namespace SamoBot.Tests;

public class UrlNormalizationRuleTests
{
    private static string Applied(IUrlRule rule, string url)
    {
        var builder = new UriBuilder(url);
        rule.Apply(builder);
        return builder.Uri.AbsoluteUri;
    }

    [Fact]
    public void LowerCaseSchemeAndHostLeavesThePathAlone() =>
        Applied(new LowerCaseSchemeAndHost(), "HTTPS://EXAMPLE.COM/Path").Should().Be("https://example.com/Path");

    [Fact]
    public void DropFragmentRemovesTheFragment() =>
        Applied(new DropFragment(), "https://example.com/a#b").Should().Be("https://example.com/a");

    [Theory]
    [InlineData("http://example.com:80/", "http://example.com/")]
    [InlineData("https://example.com:443/", "https://example.com/")]
    [InlineData("http://example.com:443/", "http://example.com:443/")]
    [InlineData("https://example.com:8443/", "https://example.com:8443/")]
    public void DropDefaultPortDropsOnlyTheDefaultOfTheScheme(string url, string expected) =>
        Applied(new DropDefaultPort(), url).Should().Be(expected);

    [Fact]
    public void RemoveTrackingParametersDropsListedNamesAndThePrefixInAnyCase() =>
        new RemoveTrackingParameters(TrackingParameters.Names, TrackingParameters.UtmPrefix)
            .Apply(["id", "FBCLID", "Utm_Whatever", "page"])
            .Should().Equal("id", "page");

    [Fact]
    public void SortParametersIgnoresCase() =>
        new SortParameters().Apply(["b", "C", "a"]).Should().Equal("a", "b", "C");
}
