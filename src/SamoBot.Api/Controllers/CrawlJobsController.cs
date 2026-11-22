using System.Security.Claims;
using FluentResults;
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
public sealed class CrawlJobsController(
    CrawlJobService service,
    ICrawlJobRepository jobs,
    IJobLinkReader links) : ControllerBase
{
    [HttpGet]
    public async Task<IActionResult> List([FromQuery] int limit = 50, CancellationToken ct = default)
    {
        var recent = await jobs.ListRecent(Math.Clamp(limit, 1, 200), ct);
        var summaries = await links.Summarize(recent.Select(job => job.Id).ToList(), ct);
        return Ok(recent.Select(job => CrawlJobDto.From(job, summaries.GetValueOrDefault(job.Id))));
    }

    [HttpGet("{id:int}")]
    public async Task<IActionResult> Get(int id, CancellationToken ct = default)
    {
        var job = await jobs.Find(id, ct);
        if (job is null)
        {
            return NotFound();
        }

        var summaries = await links.Summarize([id], ct);
        return Ok(CrawlJobDto.From(job, summaries.GetValueOrDefault(id)));
    }

    [HttpPost]
    public async Task<IActionResult> Create([FromBody] CreateCrawlJobRequest request, CancellationToken ct = default)
    {
        var owner = User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
        var input = request.ToNewJob(owner);
        var created = request.StartImmediately == true ? await service.CreateAndStart(input, ct) : await service.Create(input, ct);
        if (created.IsFailed)
        {
            return CreateFailed(created.Errors);
        }

        var response = CrawlJobDto.From(created.Value);
        return CreatedAtAction(nameof(Get), new { id = response.Id }, response);
    }

    [HttpPost("{id:int}/actions")]
    public async Task<IActionResult> Actions(int id, [FromBody] CrawlJobActionRequest request, CancellationToken ct = default)
    {
        var result = await Run(id, (request.Action ?? string.Empty).ToLowerInvariant(), ct);
        if (result is null)
        {
            return BadRequest(new ProblemDetails
            {
                Title = "Unknown action.",
                Detail = $"The action '{request.Action}' is not start, pause, cancel, recrawl, retry-failed or reprocess."
            });
        }

        return result.IsSuccess ? NoContent() : JobActionResults.Failed(result.Errors);
    }

    [HttpPost("{id:int}/links/{linkId:int}/recrawl")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    public async Task<IActionResult> Recrawl(int id, int linkId, CancellationToken ct = default)
    {
        var result = await service.Recrawl(id, RequeueScope.Link(linkId), ct);
        return result.IsSuccess ? NoContent() : JobActionResults.Failed(result.Errors);
    }

    private async Task<Result?> Run(int id, string action, CancellationToken ct) => action switch
    {
        "start" => await service.Start(id, ct),
        "pause" => await service.Pause(id, ct),
        "cancel" => await service.Cancel(id, ct),
        "recrawl" => (await service.Recrawl(id, RequeueScope.All, ct)).ToResult(),
        "retry-failed" => (await service.Recrawl(id, RequeueScope.Failed, ct)).ToResult(),
        "reprocess" => await service.Reprocess(id, ct),
        _ => null
    };

    private BadRequestObjectResult CreateFailed(IEnumerable<IError> errors)
    {
        var messages = errors.Select(error => error.Message).ToArray();
        return BadRequest(new ValidationProblemDetails(new Dictionary<string, string[]> { ["seedUrls"] = messages }));
    }
}
