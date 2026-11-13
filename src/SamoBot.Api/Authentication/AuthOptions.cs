namespace SamoBot.Api.Authentication;

/// <summary>Who may sign in. Both lists empty means nobody may (fail closed).</summary>
public sealed class AuthOptions
{
    public const string SectionName = "Auth";

    public string[] AllowedEmails { get; set; } = [];

    public string[] AllowedDomains { get; set; } = [];

    /// <summary>Comma-separated alternative to <see cref="AllowedEmails"/>, for a single environment variable.</summary>
    public string AllowedEmailsCsv { get; set; } = string.Empty;

    /// <summary>Comma-separated alternative to <see cref="AllowedDomains"/>.</summary>
    public string AllowedDomainsCsv { get; set; } = string.Empty;
}

public sealed class GoogleAuthOptions
{
    public const string SectionName = "Google";

    public string ClientId { get; set; } = string.Empty;
}
