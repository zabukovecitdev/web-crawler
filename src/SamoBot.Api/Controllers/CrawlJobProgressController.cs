using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1/crawl-jobs")]
[Authorize]
public sealed class CrawlJobProgressController(IJobProgressReader progress, TimeProvider timeProvider) : ControllerBase
{
    [HttpGet("{id:int}/progress")]
    public async Task<IActionResult> Progress(int id, CancellationToken ct = default)
    {
        var data = await progress.Read(id, ct);
        return data is null ? NotFound() : Ok(CrawlProgress.From(data, timeProvider.GetUtcNow()));
    }
}
