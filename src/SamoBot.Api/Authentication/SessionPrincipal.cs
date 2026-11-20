using System.Globalization;
using System.Security.Claims;
using SamoBot.Api.Contracts;

namespace SamoBot.Api.Authentication;

public static class SessionPrincipal
{
    public const string PictureClaim = "picture";

    public const string HostedDomainClaim = "hd";

    public const string AuthTimeClaim = "auth_time";

    public static readonly TimeSpan MaxAge = TimeSpan.FromHours(24);

    public static ClaimsPrincipal Create(GoogleIdentity identity, DateTimeOffset now)
    {
        var claims = new List<Claim>
        {
            new(ClaimTypes.NameIdentifier, identity.Subject),
            new(ClaimTypes.Name, identity.Name),
            new(ClaimTypes.Email, identity.Email),
            new(AuthTimeClaim, now.ToUnixTimeSeconds().ToString(CultureInfo.InvariantCulture))
        };
        if (!string.IsNullOrEmpty(identity.PictureUrl))
        {
            claims.Add(new Claim(PictureClaim, identity.PictureUrl));
        }

        if (!string.IsNullOrEmpty(identity.HostedDomain))
        {
            claims.Add(new Claim(HostedDomainClaim, identity.HostedDomain));
        }

        return new ClaimsPrincipal(new ClaimsIdentity(claims, AuthSchemes.Cookie));
    }

    public static MeResponse ToMe(ClaimsPrincipal user) => new(
        user.FindFirstValue(ClaimTypes.NameIdentifier) ?? string.Empty,
        user.FindFirstValue(ClaimTypes.Name) ?? string.Empty,
        user.FindFirstValue(ClaimTypes.Email),
        user.FindFirstValue(PictureClaim));
}
