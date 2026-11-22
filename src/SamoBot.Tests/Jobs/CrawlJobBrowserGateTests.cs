using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Core.Browser;
using SamoBot.Core.Domain;
using SamoBot.Core.Jobs;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Jobs;

public class CrawlJobBrowserGateTests
{
    private readonly FakeCrawlJobRepository _jobs = new();
    private readonly FakeUrlFrontier _frontier = new();
    private readonly FakeBrowserProbe _browser = new(available: false);
    private readonly CrawlJobService _service;

    public CrawlJobBrowserGateTests()
    {
        var time = new FakeTimeProvider(TestData.Now);
        _service = new CrawlJobService(_jobs, _frontier, new FakeEventPublisher(), new BrowserGate(new BrowserAvailability(_browser, time)), new FakeSitemapSeeder(), time);
    }

    private static NewCrawlJob Request(RenderMode mode, bool jsNeeded = false) =>
        new(null, ["https://a.example/"], null, null, mode, jsNeeded, [], false, true, null);

    [Theory]
    [InlineData(RenderMode.Always, false)]
    [InlineData(RenderMode.Auto, true)]
    public async Task StartingAJobThatNeedsTheBrowserWhileItIsDownIsRefusedAndNothingIsInserted(RenderMode mode, bool jsNeeded)
    {
        var result = await _service.CreateAndStart(Request(mode, jsNeeded), default);

        result.Errors.Should().ContainSingle().Which.Should().BeOfType<BrowserUnavailableError>()
            .Which.Metadata["code"].Should().Be("browser-unavailable");
        _jobs.Saved.Should().BeEmpty();
        (await _jobs.ListRecent(10, default)).Should().BeEmpty();
    }

    [Theory]
    [InlineData(RenderMode.Never, false)]
    [InlineData(RenderMode.Never, true)]
    [InlineData(RenderMode.Auto, false)]
    public async Task AJobThatDoesNotNeedTheBrowserStartsWhileItIsDown(RenderMode mode, bool jsNeeded)
    {
        var result = await _service.CreateAndStart(Request(mode, jsNeeded), default);

        result.IsSuccess.Should().BeTrue();
    }

    [Fact]
    public async Task APlainCreateIsNotGated() =>
        (await _service.Create(Request(RenderMode.Always), default)).IsSuccess.Should().BeTrue();

    [Fact]
    public async Task StartingAnExistingJobIsRefusedWhileTheBrowserIsDownAndAllowedWhenItIsBack()
    {
        var created = await _service.Create(Request(RenderMode.Always), default);
        _jobs.Add(new CrawlJob { Id = 9, SeedUrls = ["https://a.example/"], AllowedHosts = ["a.example"], RenderMode = RenderMode.Always });

        (await _service.Start(9, default)).Errors.Should().ContainSingle().Which.Should().BeOfType<BrowserUnavailableError>();
        _browser.Available = true;
        (await _service.Start(9, default)).IsSuccess.Should().BeTrue();
        created.IsSuccess.Should().BeTrue();
    }

    [Fact]
    public async Task TheGateAsksTheBrowserAgainOnEveryStart()
    {
        _jobs.Add(new CrawlJob { Id = 9, SeedUrls = ["https://a.example/"], AllowedHosts = ["a.example"], RenderMode = RenderMode.Always });

        await _service.Start(9, default);
        await _service.Start(9, default);

        _browser.Probes.Should().Be(2);
    }
}
