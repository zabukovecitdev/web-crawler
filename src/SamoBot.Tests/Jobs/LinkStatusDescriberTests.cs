using FluentAssertions;
using SamoBot.Core.Directives;
using SamoBot.Core.Domain;
using SamoBot.Core.Jobs;

namespace SamoBot.Tests.Jobs;

public class LinkStatusDescriberTests
{
    private static JobLink Describe(LinkState state, string? failReason = null, int? statusCode = null, string? contentType = "text/html") =>
        LinkStatusDescriber.Describe(new JobLinkFacts
        {
            Id = 3,
            Url = "https://a.example/x",
            State = state,
            FailReason = failReason,
            FailCount = 2,
            StatusCode = statusCode,
            ContentType = contentType
        });

    [Fact]
    public void AnIndexedLinkHasEveryStepDone()
    {
        var link = Describe(LinkState.Indexed, statusCode: 200);

        link.Steps.Should().Be(new LinkSteps(LinkStepStatus.Done, LinkStepStatus.Done, LinkStepStatus.Done, LinkStepStatus.Done));
        link.Reason.Should().Be("In the search index.");
        link.CanRecrawl.Should().BeTrue();
    }

    [Fact]
    public void ALinkInFlightCannotBeRecrawled()
    {
        var link = Describe(LinkState.Fetching);

        link.Steps.Fetch.Should().Be(LinkStepStatus.Active);
        link.CanRecrawl.Should().BeFalse();
    }

    [Theory]
    [InlineData("http-404", 404, "The server answered 404, so there is no page to store.")]
    [InlineData("max-failures", 500, "Gave up after 5 failed attempts. The last answer was HTTP 500.")]
    [InlineData("max-failures", 0, "Gave up after 5 failed attempts. The last answer was no response (timeout or network error).")]
    public void AFailedLinkSaysWhy(string failReason, int statusCode, string expected)
    {
        var link = Describe(LinkState.Failed, failReason, statusCode);

        link.Reason.Should().Be(expected);
        link.Steps.Fetch.Should().Be(LinkStepStatus.Failed);
        link.Steps.Store.Should().Be(LinkStepStatus.Pending);
    }

    [Fact]
    public void ARetryingLinkNamesTheAttempt()
    {
        var link = Describe(LinkState.Retrying, "timeout", 0);

        link.Reason.Should().Contain("Attempt 2 of 5").And.Contain("no response");
    }

    [Theory]
    [InlineData("robots", "robots.txt")]
    [InlineData("blocked-address", "private or reserved address")]
    public void ALinkThatWasNeverRequestedHasASkippedFetch(string failReason, string expected)
    {
        var link = Describe(LinkState.Skipped, failReason);

        link.Reason.Should().Contain(expected);
        link.Steps.Fetch.Should().Be(LinkStepStatus.Skipped);
    }

    [Fact]
    public void ARedirectIsFetchedButNotStored()
    {
        var link = Describe(LinkState.Skipped, "redirect", 301);

        link.Reason.Should().StartWith("The URL redirects (301).");
        (link.Steps.Fetch, link.Steps.Store).Should().Be((LinkStepStatus.Done, LinkStepStatus.Skipped));
    }

    [Fact]
    public void AFileThatIsNotHtmlIsNotStored()
    {
        var link = Describe(LinkState.Skipped, statusCode: 200, contentType: "application/pdf");

        link.Reason.Should().Be("Not an HTML page (application/pdf). Only HTML pages are stored and indexed.");
    }

    [Fact]
    public void ANoindexPageNamesTheDirective()
    {
        var facts = new JobLinkFacts
        {
            State = LinkState.NotIndexed,
            Policy = new PagePolicy { Index = false, Directives = ["noindex (meta robots)"] }
        };

        var link = LinkStatusDescriber.Describe(facts);

        link.Reason.Should().Be("Kept out of the search index because the site asked for it: noindex (meta robots).");
        link.Steps.Index.Should().Be(LinkStepStatus.Skipped);
    }

    [Fact]
    public void EveryStateHasADescription()
    {
        foreach (var state in Enum.GetValues<LinkState>())
        {
            Describe(state).Reason.Should().NotBeNullOrWhiteSpace();
        }
    }

    [Theory]
    [InlineData("http-503", "HTTP 503")]
    [InlineData("timeout", "Timed out")]
    [InlineData(null, "Unknown error")]
    [InlineData("connection refused", "connection refused")]
    public void LabelsAreShort(string? reason, string expected) => LinkReasons.Label(reason).Should().Be(expected);
}
