using FluentAssertions;
using FluentResults;
using SamoBot.Core.Common;

namespace SamoBot.Tests;

public class CleanTests
{
    [Fact]
    public void CleanWithNullUrlShouldReturnFailure()
    {
        string? url = null;
        var result = UrlNormalizer.Clean(url!);

        result.IsFailed.Should().BeTrue();
        result.Errors.Should().ContainSingle(e => e.Message == "Invalid URL");
    }

    [Fact]
    public void CleanWithEmptyUrlShouldReturnFailure()
    {
        var url = string.Empty;

        var result = UrlNormalizer.Clean(url);

        result.IsFailed.Should().BeTrue();
        result.Errors.Should().ContainSingle(e => e.Message == "Invalid URL");
    }

    [Fact]
    public void CleanWithWhitespaceUrlShouldReturnFailure()
    {
        var url = "   ";

        var result = UrlNormalizer.Clean(url);

        result.IsFailed.Should().BeTrue();
        result.Errors.Should().ContainSingle(e => e.Message == "Invalid URL");
    }

    [Fact]
    public void CleanWithInvalidUrlShouldReturnFailure()
    {
        var url = "not-a-valid-url";
        var result = UrlNormalizer.Clean(url);

        result.IsFailed.Should().BeTrue();
        result.Errors.Should().ContainSingle(e => e.Message == "Invalid URL");
    }

    [Fact]
    public void CleanWithValidHttpUrlShouldReturnSuccess()
    {
        var url = "http://example.com";

        var result = UrlNormalizer.Clean(url);

        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be("http://example.com/");
    }

    [Fact]
    public void CleanWithValidHttpsUrlShouldReturnSuccess()
    {
        var url = "https://example.com";
        var result = UrlNormalizer.Clean(url);

        result.IsSuccess.Should().BeTrue();
        result.Value.Should().Be("https://example.com/");
    }

    [Fact]
    public void CleanShouldLowercaseScheme()
    {
        var url = "HTTP://example.com";
        var result = UrlNormalizer.Clean(url);

        result.IsSuccess.Should().BeTrue();
        result.Value.AbsoluteUri.Should().StartWith("http://");
    }

    [Fact]
    public void CleanShouldLowercaseHost()
    {
        var url = "http://EXAMPLE.COM";

        var result = UrlNormalizer.Clean(url);

        result.IsSuccess.Should().BeTrue();
        result.Value.AbsoluteUri.Should().Contain("example.com");
    }

    [Fact]
    public void CleanShouldRemoveFragment()
    {
        var url = "http://example.com/path#fragment";
        var result = UrlNormalizer.Clean(url);

        result.IsSuccess.Should().BeTrue();
        result.Value.AbsoluteUri.Should().NotContain("#");
        result.Value.AbsoluteUri.Should().Be("http://example.com/path");
    }

    [Fact]
    public void CleanShouldRemoveDefaultHttpPort()
    {
        var url = "http://example.com:80/path";
        var result = UrlNormalizer.Clean(url);

        result.IsSuccess.Should().BeTrue();
        result.Value.AbsoluteUri.Should().NotContain(":80");
        result.Value.AbsoluteUri.Should().Be("http://example.com/path");
    }

    [Fact]
    public void CleanShouldRemoveDefaultHttpsPort()
    {
        var url = "https://example.com:443/path";

        var result = UrlNormalizer.Clean(url);

        result.IsSuccess.Should().BeTrue();
        result.Value.AbsoluteUri.Should().NotContain(":443");
        result.Value.AbsoluteUri.Should().Be("https://example.com/path");
    }

    [Fact]
    public void CleanShouldKeepNonDefaultPort()
    {
        var url = "http://example.com:8080/path";

        var result = UrlNormalizer.Clean(url);

        result.IsSuccess.Should().BeTrue();
        result.Value.AbsoluteUri.Should().Contain(":8080");
        result.Value.AbsoluteUri.Should().Be("http://example.com:8080/path");
    }

    [Fact]
    public void CleanShouldKeepTrailingSlashFromPath()
    {
        var url = "http://example.com/path/";

        var result = UrlNormalizer.Clean(url);

        result.IsSuccess.Should().BeTrue();
        result.Value.AbsoluteUri.Should().EndWith("/");
        result.Value.AbsoluteUri.Should().Be("http://example.com/path/");
    }

    [Fact]
    public void CleanShouldKeepRootSlash()
    {
        var url = "http://example.com/";

        var result = UrlNormalizer.Clean(url);

        result.IsSuccess.Should().BeTrue();
        result.Value.AbsoluteUri.Should().EndWith("/");
        result.Value.AbsoluteUri.Should().Be("http://example.com/");
    }

    [Fact]
    public void CleanShouldSortQueryParameters()
    {
        var url = "http://example.com/path?zebra=1&apple=2&banana=3";

        var result = UrlNormalizer.Clean(url);

        result.IsSuccess.Should().BeTrue();
        var queryString = result.Value.AbsoluteUri.Split('?')[1];
        var parameters = queryString.Split('&');
        parameters[0].Should().StartWith("apple=");
        parameters[1].Should().StartWith("banana=");
        parameters[2].Should().StartWith("zebra=");
    }

    [Fact]
    public void CleanShouldSortQueryParametersCaseInsensitive()
    {
        var url = "http://example.com/path?Zebra=1&apple=2";

        var result = UrlNormalizer.Clean(url);

        result.IsSuccess.Should().BeTrue();
        var queryString = result.Value.AbsoluteUri.Split('?')[1];
        var parameters = queryString.Split('&');
        parameters[0].Should().StartWith("apple=");
        parameters[1].Should().StartWith("Zebra=");
    }

    [Fact]
    public void CleanShouldPreserveQueryParameterValues()
    {
        var url = "http://example.com/path?key1=value1&key2=value2";

        var result = UrlNormalizer.Clean(url);

        result.IsSuccess.Should().BeTrue();
        result.Value.AbsoluteUri.Should().Contain("key1=value1");
        result.Value.AbsoluteUri.Should().Contain("key2=value2");
    }

    [Fact]
    public void CleanWithComplexUrlShouldNormalizeAllAspects()
    {
        var url = "HTTPS://EXAMPLE.COM:443/path/to/resource/?zebra=1&apple=2#fragment";
        var result = UrlNormalizer.Clean(url);

        result.IsSuccess.Should().BeTrue();
        result.Value.AbsoluteUri.Should().Be("https://example.com/path/to/resource/?apple=2&zebra=1");
    }

    [Fact]
    public void CleanWithUrlContainingSpecialCharactersShouldPreserveThem()
    {
        var url = "http://example.com/path?key=value%20with%20spaces&other=test";
        var result = UrlNormalizer.Clean(url);

        result.IsSuccess.Should().BeTrue();
        result.Value.AbsoluteUri.Should().Contain("key=value+with+spaces");
        result.Value.AbsoluteUri.Should().Contain("other=test");
    }
}
