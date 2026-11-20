namespace SamoBot.Api.Authentication;

public sealed record GoogleIdentity(
    string Subject,
    string Email,
    bool EmailVerified,
    string Name,
    string? PictureUrl,
    string? HostedDomain);
