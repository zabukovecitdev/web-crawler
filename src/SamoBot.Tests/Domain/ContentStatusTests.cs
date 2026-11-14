using FluentAssertions;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Domain;

public class ContentStatusTests
{
    private static FetchResponse Response(int status) => new() { StatusCode = status, ContentType = "text/html" };

    [Fact]
    public void ANetworkErrorIsFailed() => ContentStatus.Of(Response(0), null).Should().Be("Failed");

    [Fact]
    public void ASavedBodyIsStored() => ContentStatus.Of(Response(200), "obj").Should().Be("Stored");

    [Fact]
    public void ASuccessWithoutASavedBodyIsSkipped() => ContentStatus.Of(Response(200), null).Should().Be("Skipped");

    [Theory]
    [InlineData(301)]
    [InlineData(404)]
    [InlineData(503)]
    public void RedirectsAndErrorStatusesAreNotStored(int status) => ContentStatus.Of(Response(status), null).Should().Be("NotStored");
}
