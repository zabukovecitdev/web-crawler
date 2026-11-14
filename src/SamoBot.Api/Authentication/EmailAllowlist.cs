using Microsoft.Extensions.Options;

namespace SamoBot.Api.Authentication;

/// <summary>Decides whether a verified Google identity may sign in. Empty lists deny everybody.</summary>
public sealed class EmailAllowlist
{
    private static readonly char[] Separators = [',', ';', ' ', '\n', '\r', '\t'];

    private readonly HashSet<string> _emails;
    private readonly HashSet<string> _domains;

    public EmailAllowlist(IOptions<AuthOptions> options)
    {
        var value = options.Value;
        _emails = Normalize(value.AllowedEmails, value.AllowedEmailsCsv, false);
        _domains = Normalize(value.AllowedDomains, value.AllowedDomainsCsv, true);
    }

    public bool IsEmpty => _emails.Count == 0 && _domains.Count == 0;

    public bool IsAllowed(GoogleIdentity identity)
    {
        if (!identity.EmailVerified || string.IsNullOrWhiteSpace(identity.Email))
        {
            return false;
        }

        var email = identity.Email.Trim().ToLowerInvariant();
        return _emails.Contains(email)
               || _domains.Contains(DomainOf(email))
               || (!string.IsNullOrWhiteSpace(identity.HostedDomain)
                   && _domains.Contains(identity.HostedDomain.Trim().ToLowerInvariant()));
    }

    private static string DomainOf(string email)
    {
        var at = email.LastIndexOf('@');
        return at < 0 ? string.Empty : email[(at + 1)..];
    }

    private static HashSet<string> Normalize(string[] values, string csv, bool domains)
    {
        var all = values.Concat(csv.Split(Separators, StringSplitOptions.RemoveEmptyEntries));
        var cleaned = all
            .Select(v => v.Trim().ToLowerInvariant())
            .Select(v => domains ? v.TrimStart('@') : v)
            .Where(v => v.Length > 0);
        return new HashSet<string>(cleaned);
    }
}
