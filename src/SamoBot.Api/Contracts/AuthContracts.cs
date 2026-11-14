namespace SamoBot.Api.Contracts;

public sealed record GoogleSignInRequest(string? Credential);

public sealed record MeResponse(string UserId, string Name, string? Email, string? PictureUrl);
