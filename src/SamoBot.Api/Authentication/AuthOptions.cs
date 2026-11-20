namespace SamoBot.Api.Authentication;

public sealed class AuthOptions
{
    public const string SectionName = "Auth";

    public string[] AllowedEmails { get; set; } = [];

    public string[] AllowedDomains { get; set; } = [];

    public string AllowedEmailsCsv { get; set; } = string.Empty;

    public string AllowedDomainsCsv { get; set; } = string.Empty;

    public bool AllowInsecureCookie { get; set; }
}

public sealed class GoogleAuthOptions
{
    public const string SectionName = "Google";

    public string ClientId { get; set; } = string.Empty;
}
