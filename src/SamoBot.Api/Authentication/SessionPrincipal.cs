using System.Globalization;
using System.Security.Claims;
using SamoBot.Api.Contracts;

namespace SamoBot.Api.Authentication;

/// <summary>Maps between a Google identity, the cookie session claims and the <c>Me</c> response.</summary>
public static class SessionPrincipal
{
    public const string PictureClaim = "picture";

    /// <summary>The Google hosted domain, kept so the allowlist can be checked again on every request.</summary>
    public const string HostedDomainClaim = "hd";

    /// <summary>Unix seconds of the sign-in. Sliding renewal keeps it, so it marks the absolute start.</summary>
    public const string AuthTimeClaim = "auth_time";

    /// <summary>A session ends this long after sign-in, however often it is used.</summary>
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
