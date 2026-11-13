using FluentAssertions;
using FluentResults;
using SamoBot.Core.Common;

namespace SamoBot.Tests;

public class TrackingParameterFilteringTests
{
    [Fact]
    public void TryCleanShouldRemoveYclid()
    {
        var url = "http://example.com/page?yclid=yandex123&search=query";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("yclid");
        cleanedUrl.AbsoluteUri.Should().Contain("search=query");
    }

    [Fact]
    public void TryCleanShouldRemoveMailchimpParameters()
    {
        var url = "http://example.com/page?mc_cid=campaign123&mc_eid=email456&product=shoes";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("mc_cid");
        cleanedUrl.AbsoluteUri.Should().NotContain("mc_eid");
        cleanedUrl.AbsoluteUri.Should().Contain("product=shoes");
    }

    [Fact]
    public void TryCleanShouldRemoveMarketoParameter()
    {
        var url = "http://example.com/page?mkt_tok=marketo123&category=electronics";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("mkt_tok");
        cleanedUrl.AbsoluteUri.Should().Contain("category=electronics");
    }

    [Fact]
    public void TryCleanShouldRemoveHubspotParameters()
    {
        var url = "http://example.com/page?_hsenc=hs123&_hsmi=hs456&hsCtaTracking=cta789&id=123";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("_hsenc");
        cleanedUrl.AbsoluteUri.Should().NotContain("_hsmi");
        cleanedUrl.AbsoluteUri.Should().NotContain("hsCtaTracking");
        cleanedUrl.AbsoluteUri.Should().Contain("id=123");
    }

    [Fact]
    public void TryCleanShouldRemoveGeneralTrackingParameters()
    {
        var url = "http://example.com/page?trk=track123&trk_info=info456&ncid=ncid789&id=123";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("trk");
        cleanedUrl.AbsoluteUri.Should().NotContain("trk_info");
        cleanedUrl.AbsoluteUri.Should().NotContain("ncid");
        cleanedUrl.AbsoluteUri.Should().Contain("id=123");
    }

    [Fact]
    public void TryCleanShouldRemoveClickTrackingParameters()
    {
        var url = "http://example.com/page?clickid=click123&clickId=click456&click_id=click789&product=shoes";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.ToLowerInvariant().Should().NotContain("clickid");
        cleanedUrl.AbsoluteUri.ToLowerInvariant().Should().NotContain("clickid");
        cleanedUrl.AbsoluteUri.ToLowerInvariant().Should().NotContain("click_id");
        cleanedUrl.AbsoluteUri.Should().Contain("product=shoes");
    }

    [Fact]
    public void TryCleanShouldRemoveAffiliateParameters()
    {
        var url = "http://example.com/page?affiliate=aff123&affiliateId=aff456&affiliate_id=aff789&id=123";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.ToLowerInvariant().Should().NotContain("affiliate=");
        cleanedUrl.AbsoluteUri.ToLowerInvariant().Should().NotContain("affiliateid=");
        cleanedUrl.AbsoluteUri.ToLowerInvariant().Should().NotContain("affiliate_id=");
        cleanedUrl.AbsoluteUri.Should().Contain("id=123");
    }

    [Fact]
    public void TryCleanShouldRemovePartnerParameters()
    {
        var url = "http://example.com/page?partner=part123&partnerId=part456&partner_id=part789&category=electronics";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.ToLowerInvariant().Should().NotContain("partner=");
        cleanedUrl.AbsoluteUri.ToLowerInvariant().Should().NotContain("partnerid=");
        cleanedUrl.AbsoluteUri.ToLowerInvariant().Should().NotContain("partner_id=");
        cleanedUrl.AbsoluteUri.Should().Contain("category=electronics");
    }

    [Fact]
    public void TryCleanShouldRemoveMultipleTrackingParameters()
    {
        var url = "http://example.com/page?utm_source=google&fbclid=fb123&gclid=gc456&_ga=ga789&id=123&product=shoes";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("utm_source");
        cleanedUrl.AbsoluteUri.Should().NotContain("fbclid");
        cleanedUrl.AbsoluteUri.Should().NotContain("gclid");
        cleanedUrl.AbsoluteUri.Should().NotContain("_ga");
        cleanedUrl.AbsoluteUri.Should().Contain("id=123");
        cleanedUrl.AbsoluteUri.Should().Contain("product=shoes");
        cleanedUrl.AbsoluteUri.Should().Be("http://example.com/page?id=123&product=shoes");
    }

    [Fact]
    public void TryCleanShouldPreserveNonTrackingParameters()
    {
        var url = "http://example.com/page?utm_source=google&id=123&category=electronics&sort=price";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("utm_source");
        cleanedUrl.AbsoluteUri.Should().Contain("id=123");
        cleanedUrl.AbsoluteUri.Should().Contain("category=electronics");
        cleanedUrl.AbsoluteUri.Should().Contain("sort=price");
    }

    [Fact]
    public void TryCleanShouldRemoveAllTrackingParametersAndKeepOnlyNonTracking()
    {
        var url = "http://example.com/page?utm_source=google&utm_medium=email&fbclid=fb123&gclid=gc456&product=shoes&size=large";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("utm_source");
        cleanedUrl.AbsoluteUri.Should().NotContain("utm_medium");
        cleanedUrl.AbsoluteUri.Should().NotContain("fbclid");
        cleanedUrl.AbsoluteUri.Should().NotContain("gclid");
        cleanedUrl.AbsoluteUri.Should().Contain("product=shoes");
        cleanedUrl.AbsoluteUri.Should().Contain("size=large");
        cleanedUrl.AbsoluteUri.Should().Be("http://example.com/page?product=shoes&size=large");
    }

    [Fact]
    public void TryCleanShouldHandleUrlWithOnlyTrackingParameters()
    {
        var url = "http://example.com/page?utm_source=google&fbclid=fb123";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("utm_source");
        cleanedUrl.AbsoluteUri.Should().NotContain("fbclid");
        cleanedUrl.AbsoluteUri.Should().NotContain("?");
        cleanedUrl.AbsoluteUri.Should().Be("http://example.com/page");
    }

    [Fact]
    public void TryCleanShouldHandleTrackingParametersCaseInsensitive()
    {
        var url = "http://example.com/page?FBCLID=fb123&GCLID=gc456&UTM_SOURCE=google&id=123";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.ToLowerInvariant().Should().NotContain("fbclid=");
        cleanedUrl.AbsoluteUri.ToLowerInvariant().Should().NotContain("gclid=");
        cleanedUrl.AbsoluteUri.ToLowerInvariant().Should().NotContain("utm_source=");
        cleanedUrl.AbsoluteUri.Should().Contain("id=123");
    }

    [Fact]
    public void TryCleanShouldSortRemainingParametersAfterRemovingTracking()
    {
        var url = "http://example.com/page?utm_source=google&zebra=1&apple=2&utm_medium=email&banana=3";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().NotContain("utm_source");
        cleanedUrl.AbsoluteUri.Should().NotContain("utm_medium");
        var queryString = cleanedUrl.AbsoluteUri.Split('?')[1];
        var parameters = queryString.Split('&');
        parameters[0].Should().StartWith("apple=");
        parameters[1].Should().StartWith("banana=");
        parameters[2].Should().StartWith("zebra=");
    }

    [Fact]
    public void TryCleanShouldNormalizeUrlWhileRemovingTrackingParameters()
    {
        var url = "HTTPS://EXAMPLE.COM:443/page?utm_source=google&fbclid=fb123&ID=123#fragment";

        var result = UrlNormalizer.TryClean(url, out var cleanedUrl);

        result.Should().BeTrue();
        cleanedUrl!.AbsoluteUri.Should().StartWith("https://example.com/page");
        cleanedUrl.AbsoluteUri.Should().NotContain(":443");
        cleanedUrl.AbsoluteUri.Should().NotContain("utm_source");
        cleanedUrl.AbsoluteUri.Should().NotContain("fbclid");
        cleanedUrl.AbsoluteUri.Should().NotContain("#");
        cleanedUrl.AbsoluteUri.Should().Contain("ID=123");
        cleanedUrl.AbsoluteUri.Should().Be("https://example.com/page?ID=123");
    }
}
