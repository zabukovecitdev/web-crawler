using FluentAssertions;
using FluentResults;
using SamoBot.Core.Common;

namespace SamoBot.Tests;

public class TrackingParameterRemovalTests
{
    [Fact]
    public void TryCleanShouldRemoveUtmSource()
    {
        var url = "http://example.com/page?utm_source=google&id=123";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("utm_source");
        cleanedUrl.AbsoluteUri.Should().Contain("id=123");
        cleanedUrl.AbsoluteUri.Should().Be("http://example.com/page?id=123");
    }

    [Fact]
    public void TryCleanShouldRemoveUtmMedium()
    {
        var url = "http://example.com/page?utm_medium=email&product=shoes";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("utm_medium");
        cleanedUrl.AbsoluteUri.Should().Contain("product=shoes");
        cleanedUrl.AbsoluteUri.Should().Be("http://example.com/page?product=shoes");
    }

    [Fact]
    public void TryCleanShouldRemoveUtmCampaign()
    {
        var url = "http://example.com/page?utm_campaign=summer_sale&category=electronics";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("utm_campaign");
        cleanedUrl.AbsoluteUri.Should().Contain("category=electronics");
    }

    [Fact]
    public void TryCleanShouldRemoveUtmTerm()
    {
        var url = "http://example.com/page?utm_term=keyword&search=test";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("utm_term");
        cleanedUrl.AbsoluteUri.Should().Contain("search=test");
    }

    [Fact]
    public void TryCleanShouldRemoveUtmContent()
    {
        var url = "http://example.com/page?utm_content=banner&page=1";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("utm_content");
        cleanedUrl.AbsoluteUri.Should().Contain("page=1");
    }

    [Fact]
    public void TryCleanShouldRemoveUtmId()
    {
        var url = "http://example.com/page?utm_id=12345&filter=active";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("utm_id");
        cleanedUrl.AbsoluteUri.Should().Contain("filter=active");
    }

    [Fact]
    public void TryCleanShouldRemoveAllUtmParameters()
    {
        var url = "http://example.com/page?utm_source=google&utm_medium=cpc&utm_campaign=test&utm_term=keyword&utm_content=ad&id=123";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("utm_source");
        cleanedUrl.AbsoluteUri.Should().NotContain("utm_medium");
        cleanedUrl.AbsoluteUri.Should().NotContain("utm_campaign");
        cleanedUrl.AbsoluteUri.Should().NotContain("utm_term");
        cleanedUrl.AbsoluteUri.Should().NotContain("utm_content");
        cleanedUrl.AbsoluteUri.Should().Contain("id=123");
        cleanedUrl.AbsoluteUri.Should().Be("http://example.com/page?id=123");
    }

    [Fact]
    public void TryCleanShouldRemoveUtmParametersCaseInsensitive()
    {
        var url = "http://example.com/page?UTM_SOURCE=google&Utm_Medium=cpc&id=123";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.ToLowerInvariant().Should().NotContain("utm_source");
        cleanedUrl.AbsoluteUri.ToLowerInvariant().Should().NotContain("utm_medium");
        cleanedUrl.AbsoluteUri.Should().Contain("id=123");
    }

    [Fact]
    public void TryCleanShouldRemoveUtmCustomParameters()
    {
        var url = "http://example.com/page?utm_custom_param=value&id=123";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("utm_custom_param");
        cleanedUrl.AbsoluteUri.Should().Contain("id=123");
    }

    [Fact]
    public void TryCleanShouldRemoveFbclid()
    {
        var url = "http://example.com/page?fbclid=abc123&product=shoes";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("fbclid");
        cleanedUrl.AbsoluteUri.Should().Contain("product=shoes");
    }

    [Fact]
    public void TryCleanShouldRemoveGclid()
    {
        var url = "http://example.com/page?gclid=xyz789&category=electronics";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("gclid");
        cleanedUrl.AbsoluteUri.Should().Contain("category=electronics");
    }

    [Fact]
    public void TryCleanShouldRemoveIgshid()
    {
        var url = "http://example.com/page?igshid=insta123&page=home";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("igshid");
        cleanedUrl.AbsoluteUri.Should().Contain("page=home");
    }

    [Fact]
    public void TryCleanShouldRemoveTwclid()
    {
        var url = "http://example.com/page?twclid=twitter456&filter=active";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("twclid");
        cleanedUrl.AbsoluteUri.Should().Contain("filter=active");
    }

    [Fact]
    public void TryCleanShouldRemoveLiFatId()
    {
        var url = "http://example.com/page?li_fat_id=linkedin789&sort=price";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("li_fat_id");
        cleanedUrl.AbsoluteUri.Should().Contain("sort=price");
    }

    [Fact]
    public void TryCleanShouldRemoveGoogleAnalyticsParameters()
    {
        var url = "http://example.com/page?_ga=GA1.2.123456&_gl=1.abc123&id=123";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("_ga");
        cleanedUrl.AbsoluteUri.Should().NotContain("_gl");
        cleanedUrl.AbsoluteUri.Should().Contain("id=123");
    }
}
