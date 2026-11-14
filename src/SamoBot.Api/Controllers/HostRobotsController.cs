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
public sealed class HostRobotsController(RobotsGate robots) : ControllerBase
{
    [HttpGet("{host}/robots")]
    [ProducesResponseType<RobotsResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> GetRobots(string host, CancellationToken ct) =>
        HostKey.TryNormalize(host, out var key)
            ? Ok(RobotsResponse.From(await robots.RulesFor(key, ct)))
            : BadRequest("Invalid host.");

    [HttpPost("explain")]
    [ProducesResponseType<ExplainResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> Explain([FromBody] ExplainRequest request, CancellationToken ct)
    {
        if (!Uri.TryCreate(request.Url, UriKind.Absolute, out var uri) || uri.Scheme is not ("http" or "https"))
        {
            return BadRequest("Invalid URL.");
        }

        var rules = await robots.RulesFor(uri.Authority, ct);
        var description = RobotsResponse.From(rules);
        return Ok(new ExplainResponse(
            request.Url,
            uri.Authority,
            RobotsGate.UserAgent,
            rules.IsAllowed(uri.PathAndQuery, RobotsGate.UserAgent),
            description.CrawlDelayMs,
            description));
    }
}
