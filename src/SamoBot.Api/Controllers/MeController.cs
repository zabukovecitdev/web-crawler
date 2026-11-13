using System.Security.Claims;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1")]
[Authorize]
public class MeController : ControllerBase
{
    [HttpGet("me")]
    [ProducesResponseType<MeResponse>(StatusCodes.Status200OK)]
    public IActionResult Me()
    {
        var id = User.FindFirstValue(ClaimTypes.NameIdentifier);
        var name = User.FindFirstValue(ClaimTypes.Name);
        return Ok(new MeResponse(id, name));
    }
}
