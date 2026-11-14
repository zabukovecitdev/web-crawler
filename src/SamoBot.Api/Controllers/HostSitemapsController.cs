using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Core.Sitemaps;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1/hosts")]
[Authorize]
public sealed class HostSitemapsController(SitemapOverviewService sitemaps) : ControllerBase
{
    private const int MaxLimit = 200;

    /// <summary>A host that has no stored sitemap gives an empty result, not 404.</summary>
    [HttpGet("{host}/sitemaps")]
    [ProducesResponseType<SitemapDto>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> GetSitemaps(
        string host,
        [FromQuery] int limit = 50,
        [FromQuery] int offset = 0,
        CancellationToken ct = default)
    {
        if (!HostKey.TryNormalize(host, out var key))
        {
            return BadRequest("Invalid host.");
        }

        if (offset > ListLimits.MaxOffset)
        {
            return BadRequest($"Offset must be between 0 and {ListLimits.MaxOffset}.");
        }

        var overview = await sitemaps.Read(key, Math.Clamp(limit, 1, MaxLimit), Math.Max(offset, 0), ct);
        return Ok(SitemapDto.From(overview));
    }
}
