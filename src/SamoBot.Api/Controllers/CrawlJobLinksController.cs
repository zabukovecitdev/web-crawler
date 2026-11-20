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
public sealed class CrawlJobLinksController(
    ICrawlJobRepository jobs,
    IJobLinkReader links) : ControllerBase
{
    [HttpGet("{id:int}/links")]
    [ProducesResponseType<PagedResult<JobLink>>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> List(
        int id,
        [FromQuery] string? filter = null,
        [FromQuery] string? query = null,
        [FromQuery] int limit = 50,
        [FromQuery] int offset = 0,
        CancellationToken ct = default)
    {
        var parsed = JobLinkQueries.Parse(filter, query, limit, offset);
        if (parsed.Query is null)
        {
            return BadRequest(parsed.Error);
        }

        if (await jobs.Find(id, ct) is null)
        {
            return NotFound();
        }

        var page = await links.List(id, parsed.Query, ct);
        return Ok(new PagedResult<JobLink>(page.Total, page.Items.Select(LinkStatusDescriber.Describe).ToList()));
    }
}
