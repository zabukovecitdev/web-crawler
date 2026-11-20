using FluentAssertions;
using SamoBot.Core.Directives;
using SamoBot.Core.Directives.Rules;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Directives;

public class PagePolicyRuleTests
{
    private static readonly PageContext Context = TestDirectives.Context(new ParsedPage());

    private static PagePolicy Run(IPagePolicyRule rule, params string[] directives) =>
        rule.Apply(PagePolicy.Default, Context, directives.SelectMany(d => RobotsDirectiveParser.ParseList(d, "test")).ToList());

    [Fact]
    public void TheDefaultPolicyAllowsEverything()
    {
        var policy = PagePolicy.Default;

        policy.FollowLinks.Should().BeTrue();
        policy.Index.Should().BeTrue();
        policy.Archive.Should().BeTrue();
        policy.SnippetMaxLength.Should().BeNull();
        policy.IndexUntil.Should().BeNull();
    }

    [Theory]
    [InlineData("nofollow", false)]
    [InlineData("none", false)]
    [InlineData("noindex", true)]
    [InlineData("nofollowing", true)]
    public void NoFollow(string directive, bool follows) => Run(new NoFollowRule(), directive).FollowLinks.Should().Be(follows);

    [Theory]
    [InlineData("noindex", false)]
    [InlineData("none", false)]
    [InlineData("nofollow", true)]
    [InlineData("index", true)]
    public void NoIndex(string directive, bool indexes) => Run(new NoIndexRule(), directive).Index.Should().Be(indexes);

    [Fact]
    public void NoArchive() => Run(new NoArchiveRule(), "noarchive").Archive.Should().BeFalse();

    [Fact]
    public void NoSnippetMeansASnippetOfZero() => Run(new NoSnippetRule(), "nosnippet").SnippetMaxLength.Should().Be(0);

    [Theory]
    [InlineData("max-snippet: 50", 50)]
    [InlineData("max-snippet: 0", 0)]
    [InlineData("max-snippet: -1", null)]
    [InlineData("max-snippet: lots", null)]
    public void MaxSnippet(string directive, int? expected) => Run(new MaxSnippetRule(), directive).SnippetMaxLength.Should().Be(expected);

    [Fact]
    public void TheSmallestSnippetLimitWins() =>
        Run(new MaxSnippetRule(), "max-snippet: 80", "max-snippet: 30", "max-snippet: 60").SnippetMaxLength.Should().Be(30);

    [Fact]
    public void UnavailableAfterKeepsTheEarliestDate() =>
        Run(new UnavailableAfterRule(), "unavailable_after: 2027-03-01", "unavailable_after: 2026-12-31T10:00:00Z", "unavailable_after: soon")
            .IndexUntil.Should().Be(new DateTimeOffset(2026, 12, 31, 10, 0, 0, TimeSpan.Zero));

    [Fact]
    public void IndexingEndsAtTheUnavailableAfterTime()
    {
        var until = new DateTimeOffset(2026, 12, 31, 0, 0, 0, TimeSpan.Zero);
        var policy = PagePolicy.Default with { IndexUntil = until };

        policy.AllowsIndexingAt(until.AddSeconds(-1)).Should().BeTrue();
        policy.AllowsIndexingAt(until).Should().BeFalse();
        (policy with { Index = false }).AllowsIndexingAt(until.AddDays(-1)).Should().BeFalse();
    }

    [Fact]
    public void TheEvaluatorCombinesEverySourceAndTheStrictestValueWins()
    {
        var page = new ParsedPage { MetaTags = [new MetaTag("robots", "max-snippet: 100, noarchive"), new MetaTag("samobot", "nosnippet")] };
        var headers = TestDirectives.Header("X-Robots-Tag", "max-snippet: 40, noindex", "googlebot: nofollow");

        var policy = TestDirectives.Evaluate(page, headers: headers);

        policy.Index.Should().BeFalse();
        policy.Archive.Should().BeFalse();
        policy.SnippetMaxLength.Should().Be(0);
        policy.FollowLinks.Should().BeTrue();
    }

    [Fact]
    public void ANewRuleOrSourcePlugsInWithoutChangingTheEvaluator()
    {
        var evaluator = new PagePolicyEvaluator([new FixedSource("noai")], [new NoAiRule()]);

        var policy = evaluator.Evaluate(Context);

        policy.Archive.Should().BeFalse();
        policy.Directives.Should().Equal("noai (fixed)");
    }

    private sealed class FixedSource(string name) : IRobotsDirectiveSource
    {
        public IEnumerable<RobotsDirective> Read(PageContext page) => [new RobotsDirective(name, null, "fixed")];
    }

    private sealed class NoAiRule() : DirectiveRule("noai")
    {
        protected override PagePolicy Apply(PagePolicy policy, RobotsDirective directive) => policy with { Archive = false };
    }
}
