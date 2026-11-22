using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Core.SiteChecks;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1/hosts/{host}/plan")]
[Authorize]
public sealed class HostPlanController(SitemapPlanner planner) : ControllerBase
{
    [HttpGet("sitemap")]
    [ProducesResponseType<SitemapPlanDto>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> Sitemap(string host, CancellationToken ct = default)
    {
        if (!HostKey.TryNormalize(host, out var key))
        {
            return BadRequest("Invalid host.");
        }

        return Ok(SitemapPlanDto.From(await planner.Ensure(key, ct)));
    }
}
