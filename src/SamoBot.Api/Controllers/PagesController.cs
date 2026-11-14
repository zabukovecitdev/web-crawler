using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1/pages")]
[Authorize]
public sealed class PagesController(IPageReader pages, IContentStore contentStore) : ControllerBase
{
    [HttpGet("{id:int}")]
    [ProducesResponseType<PageDetail>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Get(int id, CancellationToken ct)
    {
        var page = await pages.FindPage(id, ct);
        return page is null ? NotFound() : Ok(page);
    }

    [HttpGet("{id:int}/content")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetContent(int id, CancellationToken ct)
    {
        var objectName = await pages.FindContentObjectName(id, ct);
        if (objectName is null)
        {
            return NotFound();
        }

        await using var stream = await contentStore.Load(objectName, ct);
        var content = await StoredContent.Read(stream, StoredContent.MaxBytes, ct);
        Response.Headers[StoredContent.TruncatedHeader] = content.Truncated ? "true" : "false";
        return File(content.Bytes, "text/plain; charset=utf-8");
    }
}
