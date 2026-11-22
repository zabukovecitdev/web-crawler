using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Core.Scope;

namespace SamoBot.Tests.Scope;

public class PathRuleValidatorTests
{
    [Fact]
    public void NoRulesIsValid()
    {
        PathRuleValidator.Validate(null).Should().BeEmpty();
        PathRuleValidator.Validate([]).Should().BeEmpty();
    }

    [Fact]
    public void WellFormedRulesAreValid() =>
        PathRuleValidator.Validate([new PathRule(PathRuleKind.Exclude, "/blog/"), new PathRule(PathRuleKind.Include, "/a", true)])
            .Should().BeEmpty();

    [Theory]
    [InlineData("blog/", false, "A path rule must start with a slash.")]
    [InlineData("", false, "A path rule must start with a slash.")]
    [InlineData("/blog", false, "A folder rule must end with a slash.")]
    [InlineData("/blog/?x=1", false, "A path rule must not contain a query, a fragment or control characters.")]
    [InlineData("/blog/#x", false, "A path rule must not contain a query, a fragment or control characters.")]
    [InlineData("/a\nb/", false, "A path rule must not contain a query, a fragment or control characters.")]
    public void AMalformedRuleSaysWhy(string prefix, bool exact, string message) =>
        PathRuleValidator.Validate([new PathRule(PathRuleKind.Exclude, prefix, exact)]).Should().Equal(message);

    [Fact]
    public void AnUnknownKindIsRefused() =>
        PathRuleValidator.Validate([new PathRule((PathRuleKind)9, "/a/")]).Should().Equal("A path rule is neither include nor exclude.");

    [Fact]
    public void ATooLongPrefixIsRefused() =>
        PathRuleValidator.Validate([new PathRule(PathRuleKind.Exclude, "/" + new string('a', 600) + "/")])
            .Should().Equal($"A path rule is longer than {PathRuleValidator.MaxPrefixLength} characters.");

    [Fact]
    public void TooManyRulesAreRefused()
    {
        var rules = Enumerable.Range(0, PathRuleValidator.MaxRules + 1).Select(i => new PathRule(PathRuleKind.Exclude, $"/{i}/")).ToList();

        PathRuleValidator.Validate(rules).Should().Contain($"At most {PathRuleValidator.MaxRules} path rules are allowed.");
    }
}
