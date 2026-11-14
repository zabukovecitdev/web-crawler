using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1/search")]
[Authorize]
public sealed class SearchController(ISearchPages search) : ControllerBase
{
    [HttpGet]
    [ProducesResponseType<SearchResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> Search(
        [FromQuery] string? q,
        [FromQuery] string? host,
        [FromQuery] int limit = 20,
        [FromQuery] int offset = 0,
        CancellationToken ct = default)
    {
        if (!SearchRequest.TryCreate(q, host, limit, offset, out var request, out var error))
        {
            return BadRequest(error);
        }

        return Ok(SearchResponse.From(await search.Search(request!, ct)));
    }
}
