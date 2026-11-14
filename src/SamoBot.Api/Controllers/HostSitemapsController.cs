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
    public async Task<IActionResult> GetSitemaps(
        string host,
        [FromQuery] int limit = 50,
        [FromQuery] int offset = 0,
        CancellationToken ct = default)
    {
        var overview = await sitemaps.Read(host.ToLowerInvariant(), Math.Clamp(limit, 1, MaxLimit), Math.Max(offset, 0), ct);
        return Ok(SitemapDto.From(overview));
    }
}
