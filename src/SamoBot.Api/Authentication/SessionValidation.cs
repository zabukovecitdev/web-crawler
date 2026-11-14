using System.Globalization;
using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;

namespace SamoBot.Api.Authentication;

/// <summary>
/// Runs on every cookie-authenticated request: the account must still be on the allowlist and the
/// session must be younger than <see cref="SessionPrincipal.MaxAge"/>. The check is an in-memory lookup.
/// </summary>
public static class SessionValidation
{
    private const long MaxUnixSeconds = 253_402_300_799;

    public static async Task ValidateAsync(CookieValidatePrincipalContext context)
    {
        var services = context.HttpContext.RequestServices;
        var allowlist = services.GetRequiredService<EmailAllowlist>();
        var now = services.GetRequiredService<TimeProvider>().GetUtcNow();
        if (IsValid(context.Principal, allowlist, now))
        {
            return;
        }

        context.RejectPrincipal();
        await context.HttpContext.SignOutAsync(AuthSchemes.Cookie);
    }

    public static bool IsValid(ClaimsPrincipal? principal, EmailAllowlist allowlist, DateTimeOffset now)
    {
        if (principal is null || !TryReadAuthTime(principal, out var signedInAt))
        {
            return false;
        }

        return now - signedInAt <= SessionPrincipal.MaxAge
               && allowlist.IsAllowed(
                   principal.FindFirstValue(ClaimTypes.Email),
                   principal.FindFirstValue(SessionPrincipal.HostedDomainClaim));
    }

    private static bool TryReadAuthTime(ClaimsPrincipal principal, out DateTimeOffset signedInAt)
    {
        signedInAt = default;
        var raw = principal.FindFirstValue(SessionPrincipal.AuthTimeClaim);
        var parsed = long.TryParse(raw, NumberStyles.None, CultureInfo.InvariantCulture, out var seconds);
        if (!parsed || seconds > MaxUnixSeconds)
        {
            return false;
        }

        signedInAt = DateTimeOffset.FromUnixTimeSeconds(seconds);
        return true;
    }
}
