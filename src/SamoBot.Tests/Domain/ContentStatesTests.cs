using FluentAssertions;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Domain;

public class ContentStatesTests
{
    private static readonly PageStateFacts StoredHtml = new() { StatusCode = 200, ContentType = "text/html", ObjectName = "obj" };

    [Fact]
    public void ANeverFetchedUrlIsNotStoredUnlessItHasAFailReason()
    {
        ContentStates.Derive(new PageStateFacts()).Should().Be(ContentState.NotStored);
        ContentStates.Derive(new PageStateFacts { FailReason = "dns" }).Should().Be(ContentState.Failed);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(199)]
    [InlineData(404)]
    [InlineData(503)]
    public void AnErrorStatusIsFailed(int status) =>
        ContentStates.Derive(StoredHtml with { StatusCode = status }).Should().Be(ContentState.Failed);

    [Theory]
    [InlineData(301)]
    [InlineData(399)]
    public void ARedirectStatusIsRedirect(int status) =>
        ContentStates.Derive(StoredHtml with { StatusCode = status }).Should().Be(ContentState.Redirect);

    [Fact]
    public void ASuccessWithoutAStoredObjectIsSkippedForNonHtmlAndNotStoredForHtml()
    {
        ContentStates.Derive(StoredHtml with { ObjectName = null, ContentType = "application/pdf" }).Should().Be(ContentState.Skipped);
        ContentStates.Derive(StoredHtml with { ObjectName = null }).Should().Be(ContentState.NotStored);
    }

    [Fact]
    public void AStoredPageMovesFromWaitingForParseToIndexed()
    {
        ContentStates.Derive(StoredHtml).Should().Be(ContentState.WaitingForParse);
        ContentStates.Derive(StoredHtml with { HasParsedDocument = true }).Should().Be(ContentState.ParsedWaitingForIndex);
        ContentStates.Derive(StoredHtml with { HasParsedDocument = true, Indexed = true }).Should().Be(ContentState.ParsedIndexed);
    }
}
