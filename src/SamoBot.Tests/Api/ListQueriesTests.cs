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
        ListQueries.Sites(null, sort, 10, 0).Query!.Sort.Should().Be(expected);

    [Fact]
    public void SitesRejectAnUnknownSortAndClampPaging()
    {
        ListQueries.Sites(null, "bogus", 10, 0).Error.Should().Be("Unknown sort.");

        var query = ListQueries.Sites("  a ", null, 5000, 3);

        query.Query.Should().Be(new SiteQuery("a", SiteSort.Host, 200, 3));
    }

    [Theory]
    [InlineData(null, PageStateFilter.All)]
    [InlineData("ok", PageStateFilter.Ok)]
    [InlineData("redirect", PageStateFilter.Redirect)]
    [InlineData("error", PageStateFilter.Error)]
    public void PagesParseTheState(string? state, PageStateFilter expected) =>
        ListQueries.Pages(null, state, 10, 0).Query!.State.Should().Be(expected);

    [Fact]
    public void PagesRejectAnUnknownStateAndTurnBlankTextIntoNoFilter()
    {
        ListQueries.Pages(null, "bogus", 10, 0).Error.Should().Be("Unknown state.");
        ListQueries.Pages("   ", null, 0, 0).Query.Should().Be(new PageQuery(null, PageStateFilter.All, 1, 0));
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(ListLimits.MaxOffset + 1)]
    public void ListsRejectAnOffsetOutsideTheCap(int offset)
    {
        ListQueries.Sites(null, null, 10, offset).Error.Should().Be("Offset must be between 0 and 100000.");
        ListQueries.Pages(null, null, 10, offset).Error.Should().Be("Offset must be between 0 and 100000.");
    }

    [Fact]
    public void ListsAcceptTheLargestOffsetAndQueryAndRejectALongerQuery()
    {
        ListQueries.Sites(new string('a', ListLimits.MaxQueryLength), null, 10, ListLimits.MaxOffset).Error.Should().BeNull();
        ListQueries.Sites(new string('a', ListLimits.MaxQueryLength + 1), null, 10, 0).Error
            .Should().Be("Query is longer than 200 characters.");
        ListQueries.Pages(new string('a', ListLimits.MaxQueryLength + 1), null, 10, 0).Error
            .Should().Be("Query is longer than 200 characters.");
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
