using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Core.Jobs;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1/crawl-jobs/{id:int}")]
[Authorize]
public sealed class CrawlJobTreeController(JobTreeService trees) : ControllerBase
{
    [HttpGet("tree")]
    [ProducesResponseType<PathTreeDto>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Tree(int id, [FromQuery] string parent = "/", [FromQuery] int depth = 2, CancellationToken ct = default)
    {
        var tree = await trees.Tree(id, parent, Math.Clamp(depth, 1, 2), ct);
        if (tree.IsSuccess)
        {
            return Ok(PathTreeDto.From(tree.Value));
        }

        return tree.Errors.Any(error => error is JobNotFoundError) ? NotFound() : BadRequest(string.Join(" ", tree.Errors.Select(error => error.Message)));
    }
}
