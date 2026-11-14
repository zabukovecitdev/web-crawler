using Microsoft.Extensions.Options;

namespace SamoBot.Api.Authentication;

/// <summary>Warns once at startup when nobody can sign in.</summary>
public sealed partial class AllowlistStartupCheck : IHostedService
{
    private readonly EmailAllowlist _allowlist;
    private readonly IOptions<GoogleAuthOptions> _google;
    private readonly ILogger<AllowlistStartupCheck> _logger;

    public AllowlistStartupCheck(
        EmailAllowlist allowlist, IOptions<GoogleAuthOptions> google, ILogger<AllowlistStartupCheck> logger)
    {
        _allowlist = allowlist;
        _google = google;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (_allowlist.IsEmpty)
        {
            LogEmptyAllowlist();
        }

        if (string.IsNullOrWhiteSpace(_google.Value.ClientId))
        {
            LogNoClientId();
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    [LoggerMessage(EventId = 1, Level = LogLevel.Warning, Message = "Auth:AllowedEmails and Auth:AllowedDomains are both empty: nobody can sign in with Google. The API key still works.")]
    private partial void LogEmptyAllowlist();

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning, Message = "Google:ClientId is not set: Google sign-in is disabled.")]
    private partial void LogNoClientId();
}
