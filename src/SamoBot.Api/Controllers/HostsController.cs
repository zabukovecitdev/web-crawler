using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1/hosts")]
[Authorize]
public sealed class HostsController(ISiteReader sites) : ControllerBase
{
    [HttpGet]
    [ProducesResponseType<SitesPage>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> List(
        [FromQuery] string? query,
        [FromQuery] string? sort,
        [FromQuery] int limit = 50,
        [FromQuery] int offset = 0,
        CancellationToken ct = default)
    {
        var siteQuery = ListQueries.Sites(query, sort, limit, offset);
        return siteQuery.Query is null ? BadRequest(siteQuery.Error) : Ok(await sites.ListSites(siteQuery.Query, ct));
    }

    [HttpGet("{host}")]
    [ProducesResponseType<SiteDetail>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Get(string host, CancellationToken ct)
    {
        if (!HostKey.TryNormalize(host, out var key))
        {
            return BadRequest("Invalid host.");
        }

        var site = await sites.FindSite(key, ct);
        return site is null ? NotFound() : Ok(site);
    }
}
