using System.Text.Json;
using FluentAssertions;
using SamoBot.Adapters.Search;
using SamoBot.Api.Contracts;
using SamoBot.Core.Domain;
using SamoBot.Core.Search;

namespace SamoBot.Tests.Search;

public class SearchTests
{
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);
    private const char S = SnippetFormatter.HighlightStart;
    private const char E = SnippetFormatter.HighlightEnd;

    [Fact]
    public void SnippetKeepsMarkTagsAndEncodesEverythingElse()
    {
        var snippet = SnippetFormatter.Format($"a <script>alert(1)</script> & {S}match{E} \"q\" 'x'");

        snippet.Should().Be("a &lt;script&gt;alert(1)&lt;/script&gt; &amp; <mark>match</mark> &quot;q&quot; &#39;x&#39;");
        snippet.Replace("<mark>", "").Replace("</mark>", "").Should().NotContain("<");
    }

    [Fact]
    public void SnippetDoesNotLetTheBodyInjectMarkMarkup()
    {
        SnippetFormatter.Format("<mark onclick=x>hi</mark>").Should().Be("&lt;mark onclick=x&gt;hi&lt;/mark&gt;");
    }

    [Fact]
    public void SnippetBalancesUnmatchedHighlightTokens()
    {
        SnippetFormatter.Format($"{S}open").Should().Be("<mark>open</mark>");
        SnippetFormatter.Format($"close{E}").Should().Be("close");
        SnippetFormatter.Format($"{S}a{S}b{E}{E}").Should().Be("<mark>ab</mark>");
    }

    [Fact]
    public void SnippetOfNothingIsEmptyAndWhitespaceIsCollapsedToSpaces()
    {
        SnippetFormatter.Format(null).Should().BeEmpty();
        SnippetFormatter.Format("a\nb\tc").Should().Be("a b c");
    }

    [Fact]
    public void PickPrefersTheTextThatHasAMatch()
    {
        SnippetFormatter.Pick("plain body", $"{S}hit{E} description").Should().Be("<mark>hit</mark> description");
        SnippetFormatter.Pick(null, "plain description").Should().Be("plain description");
        SnippetFormatter.Pick(null, null).Should().BeEmpty();
    }

    [Fact]
    public void ValidRequestIsTrimmedAndTheHostLowerCased()
    {
        var ok = SearchRequest.TryCreate("  cats ", " Example.COM ", 10, 5, out var request, out var error);

        ok.Should().BeTrue();
        error.Should().BeEmpty();
        request.Should().Be(new SearchRequest("cats", "example.com", 10, 5));
    }

    [Fact]
    public void BlankHostMeansNoHostFilter()
    {
        SearchRequest.TryCreate("cats", "  ", 10, 0, out var request, out _);

        request!.Host.Should().BeNull();
    }

    [Theory]
    [InlineData(null, 10, 0)]
    [InlineData("  ", 10, 0)]
    [InlineData("cats", 0, 0)]
    [InlineData("cats", 51, 0)]
    [InlineData("cats", 10, -1)]
    [InlineData("cats", 10, 10_001)]
    public void InvalidRequestsAreRejected(string? text, int limit, int offset)
    {
        SearchRequest.TryCreate(text, null, limit, offset, out var request, out var error).Should().BeFalse();

        request.Should().BeNull();
        error.Should().NotBeEmpty();
    }

    [Fact]
    public void TextAndHostLengthAreCapped()
    {
        SearchRequest.TryCreate(new string('a', 200), null, 10, 0, out _, out _).Should().BeTrue();
        SearchRequest.TryCreate(new string('a', 201), null, 10, 0, out _, out _).Should().BeFalse();
        SearchRequest.TryCreate("a", new string('h', 256), 10, 0, out _, out _).Should().BeFalse();
        SearchRequest.TryCreate("a", "bad\nhost", 10, 0, out _, out _).Should().BeFalse();
    }

    [Fact]
    public void HostFilterEscapesQuotesAndBackslashes()
    {
        MeilisearchPageSearch.HostFilter("a.example").Should().Be("host = \"a.example\"");
        MeilisearchPageSearch.HostFilter("x\" OR host != \"y").Should().Be("host = \"x\\\" OR host != \\\"y\"");
        MeilisearchPageSearch.HostFilter("a\\").Should().Be("host = \"a\\\\\"");
    }

    [Fact]
    public void ResponseHasTheShapeTheDashboardReads()
    {
        var page = new SearchPage(
            1,
            7,
            [new HostCount("a.example", 1)],
            [new SearchResult(4, "a.example", "https://a.example/", "T", "<mark>x</mark>", "en", null)]);

        var json = JsonSerializer.SerializeToElement(SearchResponse.From(page), Web);

        json.EnumerateObject().Select(p => p.Name).Should().BeEquivalentTo("total", "items", "tookMs", "hosts");
        json.GetProperty("items")[0].EnumerateObject().Select(p => p.Name).Should().BeEquivalentTo(
            "pageId", "host", "url", "title", "snippet", "language", "fetchedAt");
        json.GetProperty("hosts")[0].GetProperty("count").GetInt32().Should().Be(1);
    }
}
