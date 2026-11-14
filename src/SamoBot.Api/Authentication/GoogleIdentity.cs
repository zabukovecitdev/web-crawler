namespace SamoBot.Api.Authentication;

/// <summary>The verified facts we keep from a Google ID token.</summary>
public sealed record GoogleIdentity(
    string Subject,
    string Email,
    bool EmailVerified,
    string Name,
    string? PictureUrl,
    string? HostedDomain);
