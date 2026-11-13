using FluentAssertions;
using FluentResults;
using SamoBot.Core.Common;

namespace SamoBot.Tests;

public class TryCleanTests
{
    [Fact]
    public void TryCleanWithNullUrlShouldReturnFalse()
    {
        string? url = null;
        var result = UrlNormalizer.TryClean(url!, out var cleanedUrl);

        result.Should().BeFalse();
        cleanedUrl.Should().BeNull();
    }

    [Fact]
    public void TryCleanWithEmptyUrlShouldReturnFalse()
    {
        var url = string.Empty;
        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeFalse();
        cleanedUrl.Should().BeNull();
    }

    [Fact]
    public void TryCleanWithWhitespaceUrlShouldReturnFalse()
    {
        var url = "   ";
        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeFalse();
        cleanedUrl.Should().BeNull();
    }

    [Fact]
    public void TryCleanWithInvalidUrlShouldReturnFalse()
    {
        var url = "not-a-valid-url";
        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeFalse();
        cleanedUrl.Should().BeNull();
    }

    [Fact]
    public void TryCleanWithValidHttpUrlShouldReturnTrue()
    {
        var url = "http://example.com";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl.Should().Be("http://example.com/");
    }

    [Fact]
    public void TryCleanWithValidHttpsUrlShouldReturnTrue()
    {
        var url = "https://example.com";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl.Should().Be("https://example.com/");
    }

    [Fact]
    public void TryCleanShouldLowercaseScheme()
    {
        var url = "HTTP://example.com";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().StartWith("http://");
    }

    [Fact]
    public void TryCleanShouldLowercaseHost()
    {
        var url = "http://EXAMPLE.COM";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().Contain("example.com");
    }

    [Fact]
    public void TryCleanShouldRemoveFragment()
    {
        var url = "http://example.com/path#fragment";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("#");
        cleanedUrl.AbsoluteUri.Should().Be("http://example.com/path");
    }

    [Fact]
    public void TryCleanShouldRemoveDefaultHttpPort()
    {
        var url = "http://example.com:80/path";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain(":80");
        cleanedUrl.AbsoluteUri.Should().Be("http://example.com/path");
    }

    [Fact]
    public void TryCleanShouldRemoveDefaultHttpsPort()
    {
        var url = "https://example.com:443/path";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain(":443");
        cleanedUrl.AbsoluteUri.Should().Be("https://example.com/path");
    }

    [Fact]
    public void TryCleanShouldKeepNonDefaultPort()
    {
        var url = "http://example.com:8080/path";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().Contain(":8080");
        cleanedUrl.AbsoluteUri.Should().Be("http://example.com:8080/path");
    }

    [Fact]
    public void TryCleanShouldKeepTrailingSlashFromPath()
    {
        var url = "http://example.com/path/";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().EndWith("/");
        cleanedUrl.AbsoluteUri.Should().Be("http://example.com/path/");
    }

    [Fact]
    public void TryCleanShouldKeepRootSlash()
    {
        var url = "http://example.com/";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().EndWith("/");
        cleanedUrl.AbsoluteUri.Should().Be("http://example.com/");
    }

    [Fact]
    public void TryCleanShouldSortQueryParameters()
    {
        var url = "http://example.com/path?zebra=1&apple=2&banana=3";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        var queryString = cleanedUrl!.AbsoluteUri.Split('?')[1];
        var parameters = queryString.Split('&');
        parameters[0].Should().StartWith("apple=");
        parameters[1].Should().StartWith("banana=");
        parameters[2].Should().StartWith("zebra=");
    }

    [Fact]
    public void TryCleanWithComplexUrlShouldNormalizeAllAspects()
    {
        var url = "HTTPS://EXAMPLE.COM:443/path/to/resource/?zebra=1&apple=2#fragment";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().Be("https://example.com/path/to/resource/?apple=2&zebra=1");
    }

    [Fact]
    public void TryCleanAndCleanShouldProduceSameResult()
    {
        var url = "HTTPS://EXAMPLE.COM:443/path/?zebra=1&apple=2#fragment";

        var tryResult = UrlNormalizer.TryClean(url, out var tryCleanedUrl);
        var cleanResult = UrlNormalizer.Clean(url);

        tryResult.Should().BeTrue();
        cleanResult.IsSuccess.Should().BeTrue();
        tryCleanedUrl!.AbsoluteUri.Should().Be(cleanResult.Value.AbsoluteUri);
    }
}
