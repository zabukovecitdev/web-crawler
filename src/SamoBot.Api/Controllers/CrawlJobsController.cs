using System.Security.Claims;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Core.Domain;
using SamoBot.Core.Jobs;
using SamoBot.Core.Ports;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1/crawl-jobs")]
[Authorize]
public sealed class CrawlJobsController(CrawlJobService service, ICrawlJobRepository jobs, IJobPageReader pages) : ControllerBase
{
    [HttpGet]
    public async Task<IActionResult> List([FromQuery] int limit = 50, CancellationToken ct = default)
    {
        var recent = await jobs.ListRecent(Math.Clamp(limit, 1, 200), ct);
        return Ok(recent.Select(CrawlJobResponse.From));
    }

    [HttpGet("{id:int}")]
    public async Task<IActionResult> Get(int id, CancellationToken ct = default)
    {
        var job = await jobs.Find(id, ct);
        return job is null ? NotFound() : Ok(CrawlJobResponse.From(job));
    }

    [HttpPost]
    public async Task<IActionResult> Create([FromBody] CreateCrawlJobRequest request, CancellationToken ct = default)
    {
        var owner = User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
        var created = await service.Create(
            new NewCrawlJob(owner, request.SeedUrls, request.MaxDepth, request.MaxUrls, request.UseJsRendering ?? false, request.RespectRobots ?? true),
            ct);
        if (created.IsFailed)
        {
            return BadRequest(created.Errors.Select(error => error.Message));
        }

        var response = CrawlJobResponse.From(created.Value);
        return CreatedAtAction(nameof(Get), new { id = response.Id }, response);
    }

    [HttpPost("{id:int}/actions")]
    public async Task<IActionResult> Actions(int id, [FromBody] CrawlJobActionRequest request, CancellationToken ct = default)
    {
        var result = request.Action.ToLowerInvariant() switch
        {
            "start" => await service.Start(id, ct),
            "pause" => await service.Pause(id, ct),
            "cancel" => await service.Cancel(id, ct),
            _ => FluentResults.Result.Fail($"Unknown action {request.Action}.")
        };

        return result.IsSuccess ? Ok() : BadRequest();
    }

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

        return Ok(await pages.ListEvents(id, after, Math.Clamp(limit, 1, 500), ct));
    }
}
