using FluentAssertions;
using Microsoft.Playwright;
using SamoBot.Adapters.Browser;

namespace SamoBot.Tests.Browser;

public class BrowserFetcherOptionsTests
{
    [Fact]
    public void ContextsBlockServiceWorkersAndDownloads()
    {
        var options = BrowserFetcher.ContextOptions();

        options.ServiceWorkers.Should().Be(ServiceWorkerPolicy.Block);
        options.AcceptDownloads.Should().BeFalse();
    }

    [Theory]
    [InlineData("data:text/plain,hi", true)]
    [InlineData("blob:https://a.example/1", true)]
    [InlineData("about:blank", true)]
    [InlineData("ws://a.example/", false)]
    [InlineData("file:///etc/passwd", false)]
    [InlineData("ftp://a.example/", false)]
    public void OnlyNetworkFreeSchemesAreLocal(string url, bool expected) =>
        BrowserRequestGuard.IsLocalScheme(new Uri(url)).Should().Be(expected);
}
