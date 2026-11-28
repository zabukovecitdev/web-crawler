using FluentAssertions;
using SamoBot.Core.Common;

namespace SamoBot.Tests;

public class UrlNormalizationCharacterizationTests
{
    [Theory]
    [InlineData("HTTP://Example.COM:80/Path?b=2&a=1#frag", "http://example.com/Path?a=1&b=2")]
    [InlineData("https://example.com:443/", "https://example.com/")]
    [InlineData("https://example.com:8443/x/?utm_source=a&id=7&UTM_Medium=b", "https://example.com:8443/x/?id=7")]
    [InlineData("https://example.com/a/b/?Zeta=1&alpha=2&Beta=3", "https://example.com/a/b/?alpha=2&Beta=3&Zeta=1")]
    [InlineData("https://example.com/?fbclid=1&gclid=2&q=search", "https://example.com/?q=search")]
    [InlineData("https://example.com/page?q=a%20b&r=%C3%A9", "https://example.com/page?q=a+b&r=%C3%A9")]
    [InlineData("https://example.com/page?x=1&x=2", "https://example.com/page?x=1%2c2")]
    [InlineData("https://example.com/page?flag", "https://example.com/page")]
    [InlineData("https://example.com/page?=empty&k=", "https://example.com/page?empty&k=")]
    [InlineData("https://user:pw@example.com/secret", "https://user:pw@example.com/secret")]
    [InlineData("https://EXAMPLE.com/ünïcode/päth", "https://example.com/%C3%BCn%C3%AFcode/p%C3%A4th")]
    [InlineData("https://xn--bcher-kva.example/", "https://xn--bcher-kva.example/")]
    [InlineData("http://[::1]:80/ipv6", "http://[::1]/ipv6")]
    [InlineData("https://example.com/trailing/", "https://example.com/trailing/")]
    [InlineData("https://example.com/a/../b/./c", "https://example.com/b/c")]
    [InlineData("https://example.com/page?affiliateId=5&partner=x&keep=1", "https://example.com/page?keep=1")]
    [InlineData("https://example.com/page?_ga=1&_gl=2&mc_cid=3&hsCtaTracking=4", "https://example.com/page")]
    [InlineData("https://example.com/%7Euser/", "https://example.com/~user/")]
    [InlineData("https://example.com/page?a=1#", "https://example.com/page?a=1")]
    [InlineData("https://example.com", "https://example.com/")]
    public void NormalizedUrlsNeverChange(string url, string normalized)
    {
        UrlNormalizer.TryClean(url, out var clean).Should().BeTrue();

        clean!.AbsoluteUri.Should().Be(normalized);
    }
}
