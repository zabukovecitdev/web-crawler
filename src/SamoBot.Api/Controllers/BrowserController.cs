using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Core.Browser;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1/browser")]
[Authorize]
public sealed class BrowserController(BrowserAvailability browser) : ControllerBase
{
    [HttpGet]
    public async Task<BrowserStatusDto> Get(CancellationToken ct = default) =>
        BrowserStatusDto.From(await browser.Current(ct));

    [HttpPost("check")]
    public async Task<BrowserStatusDto> Check(CancellationToken ct = default) =>
        BrowserStatusDto.From(await browser.Refresh(ct));
}
