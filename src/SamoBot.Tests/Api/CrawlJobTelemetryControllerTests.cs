using FluentAssertions;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Api.Controllers;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Api;

public class CrawlJobTelemetryControllerTests
{
    private sealed class FakeJobPageReader : IJobPageReader
    {
        public List<string> Calls { get; } = [];

        public Task<IReadOnlyList<JobPage>> ListPages(int crawlJobId, int limit, int offset, CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<JobPage>>([]);

        public Task<int> CountPages(int crawlJobId, CancellationToken ct) => Task.FromResult(0);

        public Task<IReadOnlyList<JobEvent>> ListEvents(int crawlJobId, long afterId, int limit, CancellationToken ct)
        {
            Calls.Add($"after:{afterId}:{limit}");
            return Task.FromResult<IReadOnlyList<JobEvent>>([]);
        }

        public Task<IReadOnlyList<JobEvent>> ListRecentEvents(int crawlJobId, int limit, CancellationToken ct)
        {
            Calls.Add($"recent:{limit}");
            return Task.FromResult<IReadOnlyList<JobEvent>>([]);
        }
    }

    private readonly FakeJobPageReader _reader = new();
    private readonly CrawlJobTelemetryController _controller;

    public CrawlJobTelemetryControllerTests()
    {
        var jobs = new FakeCrawlJobRepository();
        jobs.Add(new CrawlJob { Id = 5 });
        _controller = new CrawlJobTelemetryController(jobs, _reader);
    }

    [Fact]
    public async Task EventsWithoutACursorUseTheTailQuery()
    {
        await _controller.Events(5, null, 100, default);

        _reader.Calls.Should().Equal("recent:100");
    }

    [Fact]
    public async Task EventsAfterZeroStartFromTheBeginning()
    {
        await _controller.Events(5, 0, 100, default);

        _reader.Calls.Should().Equal("after:0:100");
    }

    [Fact]
    public async Task ANegativeCursorCountsAsZeroAndTheLimitIsClamped()
    {
        await _controller.Events(5, -9, 5000, default);

        _reader.Calls.Should().Equal("after:0:500");
    }

    [Fact]
    public async Task EventsOfAnUnknownJobAreNotFound() =>
        (await _controller.Events(6, null, 100, default)).Should().BeOfType<NotFoundResult>();

    [Fact]
    public async Task PagesRejectAnOffsetAboveTheCap() =>
        (await _controller.Pages(5, 50, ListLimits.MaxOffset + 1, default)).Should().BeOfType<BadRequestObjectResult>();
}
