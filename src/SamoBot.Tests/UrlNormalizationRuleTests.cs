using FluentAssertions;
using SamoBot.Core.Common;

namespace SamoBot.Tests;

public class UrlNormalizationRuleTests
{
    private static UriBuilder Applied(IUrlRule rule, UriBuilder url)
    {
        rule.Apply(url);
        return url;
    }

    [Fact]
    public void LowerCaseSchemeAndHostLeavesThePathAlone()
    {
        var url = Applied(new LowerCaseSchemeAndHost(), new UriBuilder { Scheme = "HTTPS", Host = "EXAMPLE.COM", Path = "/Path" });

        (url.Scheme, url.Host, url.Path).Should().Be(("https", "example.com", "/Path"));
    }

    [Fact]
    public void DropFragmentRemovesTheFragment() =>
        Applied(new DropFragment(), new UriBuilder("https://example.com/a#b")).Fragment.Should().BeEmpty();

    [Theory]
    [InlineData("http", 80, -1)]
    [InlineData("HTTPS", 443, -1)]
    [InlineData("http", 443, 443)]
    [InlineData("https", 8443, 8443)]
    public void DropDefaultPortDropsOnlyTheDefaultOfTheScheme(string scheme, int port, int expected) =>
        Applied(new DropDefaultPort(), new UriBuilder { Scheme = scheme, Host = "example.com", Port = port }).Port.Should().Be(expected);

    [Fact]
    public void RemoveTrackingParametersDropsListedNamesAndThePrefixInAnyCase() =>
        new RemoveTrackingParameters(TrackingParameters.Names, TrackingParameters.UtmPrefix)
            .Apply(["id", "FBCLID", "Utm_Whatever", "page"])
            .Should().Equal("id", "page");

    [Fact]
    public void SortParametersIgnoresCase() =>
        new SortParameters().Apply(["b", "C", "a"]).Should().Equal("a", "b", "C");

    [Fact]
    public void RemoveTrackingParametersMatchesNamesInAnyCaseWhateverTheSetComparer() =>
        new RemoveTrackingParameters(new HashSet<string> { "gclid" }, "utm_").Apply(["GCLID", "id"]).Should().Equal("id");

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    public void RemoveTrackingParametersRefusesAnEmptyPrefix(string prefix)
    {
        var create = () => new RemoveTrackingParameters(TrackingParameters.Names, prefix);

        create.Should().Throw<ArgumentException>();
    }
}
