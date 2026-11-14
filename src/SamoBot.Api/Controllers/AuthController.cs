using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Authentication;
using SamoBot.Api.Contracts;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1/auth")]
public partial class AuthController : ControllerBase
{
    private readonly IGoogleTokenValidator _validator;
    private readonly EmailAllowlist _allowlist;
    private readonly ILogger<AuthController> _logger;

    public AuthController(IGoogleTokenValidator validator, EmailAllowlist allowlist, ILogger<AuthController> logger)
    {
        _validator = validator;
        _allowlist = allowlist;
        _logger = logger;
    }

    [HttpPost("google")]
    [AllowAnonymous]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    public async Task<IActionResult> SignInWithGoogle(GoogleSignInRequest request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.Credential))
        {
            return Problem(statusCode: StatusCodes.Status400BadRequest, title: "Missing credential");
        }

        var identity = await _validator.ValidateAsync(request.Credential, cancellationToken);
        if (identity is null)
        {
            return Problem(statusCode: StatusCodes.Status401Unauthorized, title: "Invalid Google credential");
        }

        if (!_allowlist.IsAllowed(identity))
        {
            LogDenied();
            return Problem(
                statusCode: StatusCodes.Status403Forbidden,
                title: "Access denied",
                detail: "This Google account is not allowed to use this dashboard.");
        }

        await HttpContext.SignInAsync(AuthSchemes.Cookie, SessionPrincipal.Create(identity));
        return NoContent();
    }

    [HttpPost("logout")]
    [AllowAnonymous]
    [ProducesResponseType(StatusCodes.Status204NoContent)]
    public async Task<IActionResult> Logout()
    {
        await HttpContext.SignOutAsync(AuthSchemes.Cookie);
        return NoContent();
    }

    [LoggerMessage(EventId = 1, Level = LogLevel.Warning, Message = "Sign-in denied for a Google account that is not on the allowlist.")]
    private partial void LogDenied();
}
