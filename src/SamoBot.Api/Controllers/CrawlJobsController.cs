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
    ICrawlJobRepository jobs) : ControllerBase
{
    [HttpGet]
    public async Task<IActionResult> List([FromQuery] int limit = 50, CancellationToken ct = default)
    {
        var recent = await jobs.ListRecent(Math.Clamp(limit, 1, 200), ct);
        return Ok(recent.Select(CrawlJobDto.From));
    }

    [HttpGet("{id:int}")]
    public async Task<IActionResult> Get(int id, CancellationToken ct = default)
    {
        var job = await jobs.Find(id, ct);
        return job is null ? NotFound() : Ok(CrawlJobDto.From(job));
    }

    [HttpPost]
    public async Task<IActionResult> Create([FromBody] CreateCrawlJobRequest request, CancellationToken ct = default)
    {
        var owner = User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
        var input = new NewCrawlJob(owner, request.SeedUrls, request.MaxDepth, request.MaxUrls, request.UseJsRendering ?? false, request.RespectRobots ?? true);
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
        var result = (request.Action ?? string.Empty).ToLowerInvariant() switch
        {
            "start" => await service.Start(id, ct),
            "pause" => await service.Pause(id, ct),
            "cancel" => await service.Cancel(id, ct),
            _ => null
        };

        if (result is null)
        {
            return BadRequest(new ProblemDetails { Title = "Unknown action.", Detail = $"The action '{request.Action}' is not start, pause or cancel." });
        }

        return result.IsSuccess ? NoContent() : ActionFailed(result.Errors);
    }

    private BadRequestObjectResult CreateFailed(IEnumerable<IError> errors)
    {
        var messages = errors.Select(error => error.Message).ToArray();
        return BadRequest(new ValidationProblemDetails(new Dictionary<string, string[]> { ["seedUrls"] = messages }));
    }

    private static ObjectResult ActionFailed(IEnumerable<IError> errors)
    {
        var list = errors.ToList();
        var detail = string.Join(" ", list.Select(error => error.Message));
        var notFound = list.Any(error => error is JobNotFoundError);
        var status = notFound ? StatusCodes.Status404NotFound : StatusCodes.Status409Conflict;
        return new ObjectResult(new ProblemDetails { Status = status, Title = notFound ? "Job not found." : "Action not allowed.", Detail = detail })
        {
            StatusCode = status
        };
    }
}
