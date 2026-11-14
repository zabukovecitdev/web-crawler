using System.Security.Claims;
using SamoBot.Api.Contracts;

namespace SamoBot.Api.Authentication;

/// <summary>Maps between a Google identity, the cookie session claims and the <c>Me</c> response.</summary>
public static class SessionPrincipal
{
    public const string PictureClaim = "picture";

    public static ClaimsPrincipal Create(GoogleIdentity identity)
    {
        var claims = new List<Claim>
        {
            new(ClaimTypes.NameIdentifier, identity.Subject),
            new(ClaimTypes.Name, identity.Name),
            new(ClaimTypes.Email, identity.Email)
        };
        if (!string.IsNullOrEmpty(identity.PictureUrl))
        {
            claims.Add(new Claim(PictureClaim, identity.PictureUrl));
        }

        return new ClaimsPrincipal(new ClaimsIdentity(claims, AuthSchemes.Cookie));
    }

    public static MeResponse ToMe(ClaimsPrincipal user) => new(
        user.FindFirstValue(ClaimTypes.NameIdentifier) ?? string.Empty,
        user.FindFirstValue(ClaimTypes.Name) ?? string.Empty,
        user.FindFirstValue(ClaimTypes.Email),
        user.FindFirstValue(PictureClaim));
}
