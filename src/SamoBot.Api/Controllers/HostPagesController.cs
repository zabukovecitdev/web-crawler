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
public sealed class HostPagesController(IPageReader pages) : ControllerBase
{
    [HttpGet("{host}/pages")]
    [ProducesResponseType<PagedResult<PageSummary>>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> ListPages(
        string host,
        [FromQuery] string? query,
        [FromQuery] string? state,
        [FromQuery] int limit = 50,
        [FromQuery] int offset = 0,
        CancellationToken ct = default)
    {
        if (!HostKey.TryNormalize(host, out var key))
        {
            return BadRequest("Invalid host.");
        }

        var pageQuery = ListQueries.Pages(query, state, limit, offset);
        return pageQuery.Query is null
            ? BadRequest(pageQuery.Error)
            : Ok(await pages.ListHostPages(key, pageQuery.Query, ct));
    }
}
