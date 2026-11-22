using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Core.Jobs;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1/crawl-jobs/{id:int}")]
[Authorize]
public sealed class CrawlJobPathRulesController(PathRuleService rules) : ControllerBase
{
    [HttpPut("path-rules")]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status409Conflict)]
    public async Task<IActionResult> PutRules(int id, [FromBody] PathRulesRequest request, CancellationToken ct = default)
    {
        var result = await rules.Change(id, request.Rules.Select(CreateCrawlJobMapping.ToRule).ToList(), ct);
        if (result.IsSuccess)
        {
            return NoContent();
        }

        return result.Errors.Any(error => error is InvalidRequestError)
            ? BadRequest(string.Join(" ", result.Errors.Select(error => error.Message)))
            : JobActionResults.Failed(result.Errors);
    }
}
