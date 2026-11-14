using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Api.Controllers;

/// <summary>The live view of a job: frontier pages and the event log.</summary>
[ApiController]
[Route("api/v1/crawl-jobs")]
[Authorize]
public sealed class CrawlJobTelemetryController(
    ICrawlJobRepository jobs,
    IJobPageReader pages) : ControllerBase
{
    [HttpGet("{id:int}/pages")]
    public async Task<IActionResult> Pages(
        int id, [FromQuery] int limit = 50, [FromQuery] int offset = 0, CancellationToken ct = default)
    {
        if (await jobs.Find(id, ct) is null)
        {
            return NotFound();
        }

        var items = await pages.ListPages(id, Math.Clamp(limit, 1, 200), Math.Max(0, offset), ct);
        return Ok(new PagesResponse(await pages.CountPages(id, ct), items));
    }

    [HttpGet("{id:int}/events")]
    public async Task<IActionResult> Events(
        int id, [FromQuery] long after = 0, [FromQuery] int limit = 100, CancellationToken ct = default)
    {
        if (await jobs.Find(id, ct) is null)
        {
            return NotFound();
        }

        var events = await pages.ListEvents(id, after, Math.Clamp(limit, 1, 500), ct);
        return Ok(events.Select(item => CrawlEventEnvelope.Build(item.CrawlJobId, item.EventType, item.CreatedAt, item.Payload, item.Id)));
    }
}
