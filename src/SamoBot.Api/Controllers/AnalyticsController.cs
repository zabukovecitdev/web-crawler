using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1/analytics")]
[Authorize]
public sealed class AnalyticsController(IAnalyticsReader analytics) : ControllerBase
{
    private const string BadQuery = "Unknown range or bucket.";

    [HttpGet("overview")]
    [ProducesResponseType<AnalyticsOverview>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> Overview([FromQuery] string? range, CancellationToken ct)
    {
        var query = AnalyticsQueries.Parse(range, null, null);
        return query is null ? BadRequest(BadQuery) : Ok(await analytics.Overview(query.Range, ct));
    }

    [HttpGet("hosts")]
    [ProducesResponseType<IReadOnlyList<HostAnalytics>>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> Hosts([FromQuery] string? range, CancellationToken ct)
    {
        var query = AnalyticsQueries.Parse(range, null, null);
        return query is null ? BadRequest(BadQuery) : Ok(await analytics.Hosts(query.Range, ct));
    }

    [HttpGet("status-codes")]
    [ProducesResponseType<StatusClassCounts>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> StatusCodesByClass([FromQuery] string? range, [FromQuery] string? host, CancellationToken ct)
    {
        var query = AnalyticsQueries.Parse(range, host, null);
        return query is null ? BadRequest(BadQuery) : Ok(await analytics.StatusCodes(query.Range, query.Host, ct));
    }

    [HttpGet("content-types")]
    [ProducesResponseType<IReadOnlyList<AnalyticsContentType>>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> ContentTypes([FromQuery] string? range, [FromQuery] string? host, CancellationToken ct)
    {
        var query = AnalyticsQueries.Parse(range, host, null);
        return query is null ? BadRequest(BadQuery) : Ok(await analytics.ContentTypes(query.Range, query.Host, ct));
    }

    [HttpGet("timeseries")]
    [ProducesResponseType<IReadOnlyList<TimeseriesPoint>>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> Timeseries(
        [FromQuery] string? range,
        [FromQuery] string? host,
        [FromQuery] string? bucket,
        CancellationToken ct)
    {
        var query = AnalyticsQueries.Parse(range, host, bucket);
        return query is null ? BadRequest(BadQuery) : Ok(await analytics.Timeseries(query.Range, query.Host, query.Bucket, ct));
    }
}
