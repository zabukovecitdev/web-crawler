using System.Globalization;
using FluentAssertions;
using SamoBot.Adapters.Search;
using SamoBot.Core.Directives;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Search;

public class IndexDeduplicationTests
{
    private static PageToIndex Page(int pageId, int urlId, string title) =>
        new() { PageId = pageId, UrlFetchId = pageId, DiscoveredUrlId = urlId, Title = title, Policy = PagePolicy.Default };

    [Fact]
    public void TheDocumentIdIsTheUrlId()
    {
        MeilisearchIndex.ToDocument(Page(5, 42, "t")).Id.Should().Be(42.ToString(CultureInfo.InvariantCulture));
    }

    [Fact]
    public void TwoParsesOfOneUrlGiveTheSameDocumentId()
    {
        MeilisearchIndex.ToDocument(Page(5, 42, "old")).Id.Should().Be(MeilisearchIndex.ToDocument(Page(9, 42, "new")).Id);
    }

    [Fact]
    public void ABatchKeepsTheHigherPageIdOfEachUrl()
    {
        var batch = new[] { Page(9, 1, "new"), Page(5, 1, "old"), Page(7, 2, "other") };

        var latest = MeilisearchIndex.LatestPerUrl(batch);

        latest.Should().HaveCount(2);
        latest.Single(p => p.DiscoveredUrlId == 1).Title.Should().Be("new");
        latest.Single(p => p.DiscoveredUrlId == 2).PageId.Should().Be(7);
    }
}
