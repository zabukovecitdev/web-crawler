using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Core.SiteChecks;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1/hosts/{host}/plan")]
[Authorize]
public sealed class HostPlanTreeController(PlanTreeService trees) : ControllerBase
{
    [HttpGet("tree")]
    [ProducesResponseType<PathTreeDto>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> Tree(string host, [FromQuery] string parent = "/", [FromQuery] int depth = 2, CancellationToken ct = default)
    {
        if (!HostKey.TryNormalize(host, out var key))
        {
            return BadRequest("Invalid host.");
        }

        var tree = await trees.Tree(key, parent, Math.Clamp(depth, 1, 2), ct);
        return tree.IsFailed ? BadRequest(string.Join(" ", tree.Errors.Select(error => error.Message))) : Ok(PathTreeDto.From(tree.Value));
    }
}
