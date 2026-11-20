using Microsoft.Extensions.Options;

namespace SamoBot.Api.Authentication;

public sealed class EmailAllowlist : IDisposable
{
    private readonly IDisposable? _changeRegistration;
    private volatile Snapshot _snapshot;

    public EmailAllowlist(IOptionsMonitor<AuthOptions> options)
    {
        _snapshot = Snapshot.From(options.CurrentValue);
        _changeRegistration = options.OnChange(value => _snapshot = Snapshot.From(value));
    }

    public bool IsEmpty => _snapshot.Emails.Count == 0 && _snapshot.Domains.Count == 0;

    public bool IsAllowed(GoogleIdentity identity) =>
        identity.EmailVerified && IsAllowed(identity.Email, identity.HostedDomain);

    public bool IsAllowed(string? email, string? hostedDomain)
    {
        if (string.IsNullOrWhiteSpace(email))
        {
            return false;
        }

        var current = _snapshot;
        var normalized = email.Trim().ToLowerInvariant();
        return current.Emails.Contains(normalized)
               || current.Domains.Contains(DomainOf(normalized))
               || (!string.IsNullOrWhiteSpace(hostedDomain)
                   && current.Domains.Contains(hostedDomain.Trim().ToLowerInvariant()));
    }

    public void Dispose() => _changeRegistration?.Dispose();

    private static string DomainOf(string email)
    {
        var at = email.LastIndexOf('@');
        return at < 0 ? string.Empty : email[(at + 1)..];
    }

    private sealed record Snapshot(HashSet<string> Emails, HashSet<string> Domains)
    {
        private static readonly char[] Separators = [',', ';', ' ', '\n', '\r', '\t'];

        public static Snapshot From(AuthOptions options) => new(
            Normalize(options.AllowedEmails, options.AllowedEmailsCsv, false),
            Normalize(options.AllowedDomains, options.AllowedDomainsCsv, true));

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
}
