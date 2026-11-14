using Google.Apis.Auth;
using Microsoft.Extensions.Options;

namespace SamoBot.Api.Authentication;

public interface IGoogleTokenValidator
{
    /// <summary>Returns the identity when the token is valid for our client id, otherwise null.</summary>
    Task<GoogleIdentity?> ValidateAsync(string credential, CancellationToken cancellationToken);
}

public sealed partial class GoogleTokenValidator : IGoogleTokenValidator
{
    private readonly IOptionsMonitor<GoogleAuthOptions> _options;
    private readonly ILogger<GoogleTokenValidator> _logger;

    public GoogleTokenValidator(IOptionsMonitor<GoogleAuthOptions> options, ILogger<GoogleTokenValidator> logger)
    {
        _options = options;
        _logger = logger;
    }

    public async Task<GoogleIdentity?> ValidateAsync(string credential, CancellationToken cancellationToken)
    {
        var clientId = _options.CurrentValue.ClientId;
        if (string.IsNullOrWhiteSpace(clientId))
        {
            LogNoClientId();
            return null;
        }

        try
        {
            var settings = new GoogleJsonWebSignature.ValidationSettings { Audience = [clientId] };
            var payload = await GoogleJsonWebSignature.ValidateAsync(credential, settings);
            return new GoogleIdentity(
                payload.Subject,
                payload.Email ?? string.Empty,
                payload.EmailVerified,
                payload.Name ?? payload.Email ?? payload.Subject,
                payload.Picture,
                payload.HostedDomain);
        }
        catch (InvalidJwtException ex)
        {
            LogInvalidToken(ex);
            return null;
        }
    }

    [LoggerMessage(EventId = 1, Level = LogLevel.Warning, Message = "Google sign-in rejected: Google:ClientId is not configured.")]
    private partial void LogNoClientId();

    [LoggerMessage(EventId = 2, Level = LogLevel.Information, Message = "Google sign-in rejected: the ID token is invalid.")]
    private partial void LogInvalidToken(Exception ex);
}
