using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Core.SiteChecks;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1/site-checks")]
[Authorize]
public sealed class SiteChecksController(SiteCheckService checks) : ControllerBase
{
    [HttpPost]
    [ProducesResponseType<SiteCheckDto>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> Check([FromBody] SiteCheckRequest request, CancellationToken ct = default)
    {
        var result = await checks.Check(request.Url ?? string.Empty, ct);
        if (result.IsFailed)
        {
            var messages = result.Errors.Select(error => error.Message).ToArray();
            return BadRequest(new ValidationProblemDetails(new Dictionary<string, string[]> { ["url"] = messages }));
        }

        return Ok(SiteCheckDto.From(result.Value));
    }
}
