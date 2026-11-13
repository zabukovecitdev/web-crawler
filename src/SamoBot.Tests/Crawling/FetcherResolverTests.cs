using FluentAssertions;
using SamoBot.Core.Crawling;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Crawling;

public class FetcherResolverTests
{
    [Fact]
    public void ForReturnsFirstFetcherThatSupportsTheUrl()
    {
        var first = new FakeFetcher(supports: false);
        var second = new FakeFetcher(supports: true);
        var third = new FakeFetcher(supports: true);

        new FetcherResolver([first, second, third]).For(TestData.Url()).Should().BeSameAs(second);
    }

    [Fact]
    public void ForWithoutSupportingFetcherThrows()
    {
        var resolver = new FetcherResolver([new FakeFetcher(supports: false)]);

        var act = () => resolver.For(TestData.Url());

        act.Should().Throw<InvalidOperationException>();
    }
}
