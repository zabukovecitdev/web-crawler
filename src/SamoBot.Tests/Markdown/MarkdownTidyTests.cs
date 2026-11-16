using FluentAssertions;
using SamoBot.Adapters.Markdown;

namespace SamoBot.Tests.Markdown;

public class MarkdownTidyTests
{
    [Fact]
    public void ConvertsWindowsLineEndings() =>
        MarkdownTidy.Normalize("a\r\nb").Should().Be("a\nb\n");

    [Fact]
    public void TrimsTrailingWhitespaceOfEachLine() =>
        MarkdownTidy.Normalize("a  \n\tb \t\n").Should().Be("a\n\tb\n");

    [Fact]
    public void CollapsesThreeOrMoreNewlinesToOneBlankLine() =>
        MarkdownTidy.Normalize("a\n\n\n\n\nb").Should().Be("a\n\nb\n");

    [Fact]
    public void RemovesLinesThatHoldOnlyAnEmptyLinkImageOrBullet() =>
        MarkdownTidy.Normalize("a\n[]()\n![]()\n-\nb").Should().Be("a\nb\n");

    [Fact]
    public void KeepsLinesThatHaveContent() =>
        MarkdownTidy.Normalize("- item\n[x](https://a.example/)").Should().Be("- item\n[x](https://a.example/)\n");

    [Fact]
    public void TrimsTheResultAndEndsWithExactlyOneNewline() =>
        MarkdownTidy.Normalize("\n\n  a\n\n\n").Should().Be("a\n");

    [Theory]
    [InlineData("")]
    [InlineData("  \n \n")]
    [InlineData("[]()\n-")]
    public void ReturnsAnEmptyStringWhenNothingIsLeft(string markdown) =>
        MarkdownTidy.Normalize(markdown).Should().BeEmpty();
}
