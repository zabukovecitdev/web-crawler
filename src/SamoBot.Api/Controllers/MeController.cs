using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Authentication;
using SamoBot.Api.Contracts;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1")]
[Authorize]
public class MeController : ControllerBase
{
    [HttpGet("me")]
    [ProducesResponseType<MeResponse>(StatusCodes.Status200OK)]
    public IActionResult Me() => Ok(SessionPrincipal.ToMe(User));
}
