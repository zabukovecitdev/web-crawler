using FluentAssertions;
using SamoBot.Api.Contracts;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Api;

public class ListQueriesTests
{
    [Theory]
    [InlineData(null, SiteSort.Host)]
    [InlineData("pages", SiteSort.Pages)]
    [InlineData("Failed", SiteSort.Failed)]
    [InlineData("lastCrawl", SiteSort.LastCrawl)]
    public void SitesParseTheSortName(string? sort, SiteSort expected) =>
        ListQueries.Sites(null, sort, 10, 0)!.Sort.Should().Be(expected);

    [Fact]
    public void SitesRejectAnUnknownSortAndClampPaging()
    {
        ListQueries.Sites(null, "bogus", 10, 0).Should().BeNull();

        var query = ListQueries.Sites("  a ", null, 5000, -3);

        query.Should().Be(new SiteQuery("a", SiteSort.Host, 200, 0));
    }

    [Theory]
    [InlineData(null, PageStateFilter.All)]
    [InlineData("ok", PageStateFilter.Ok)]
    [InlineData("redirect", PageStateFilter.Redirect)]
    [InlineData("error", PageStateFilter.Error)]
    public void PagesParseTheState(string? state, PageStateFilter expected) =>
        ListQueries.Pages(null, state, 10, 0)!.State.Should().Be(expected);

    [Fact]
    public void PagesRejectAnUnknownStateAndTurnBlankTextIntoNoFilter()
    {
        ListQueries.Pages(null, "bogus", 10, 0).Should().BeNull();
        ListQueries.Pages("   ", null, 0, 0).Should().Be(new PageQuery(null, PageStateFilter.All, 1, 0));
    }

    [Fact]
    public async Task StoredContentCutsABodyAtTheCapAndFlagsIt()
    {
        var capped = await StoredContent.Read(new MemoryStream(new byte[10]), 4, default);
        var whole = await StoredContent.Read(new MemoryStream(new byte[4]), 4, default);

        capped.Bytes.Should().HaveCount(4);
        capped.Truncated.Should().BeTrue();
        whole.Bytes.Should().HaveCount(4);
        whole.Truncated.Should().BeFalse();
    }
}
