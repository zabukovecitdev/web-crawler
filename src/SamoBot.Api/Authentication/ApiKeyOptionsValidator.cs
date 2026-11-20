using Microsoft.Extensions.Options;

namespace SamoBot.Api.Authentication;

public sealed class ApiKeyOptionsValidator : IValidateOptions<ApiKeyOptions>
{
    public const int MinimumLength = 32;

    private static readonly HashSet<string> Placeholders = new(StringComparer.OrdinalIgnoreCase)
    {
        "change-me", "changeme", "change_me", "secret", "password", "test", "test-key", "masterKey", "dev"
    };

    private readonly IHostEnvironment _environment;

    public ApiKeyOptionsValidator(IHostEnvironment environment)
    {
        _environment = environment;
    }

    public ValidateOptionsResult Validate(string? name, ApiKeyOptions options)
    {
        if (!_environment.IsProduction() || string.IsNullOrEmpty(options.Key))
        {
            return ValidateOptionsResult.Success;
        }

        var weak = options.Key.Length < MinimumLength || Placeholders.Contains(options.Key);
        return weak
            ? ValidateOptionsResult.Fail(
                "DashboardApi:Key is a placeholder or shorter than 32 characters. Generate one with: openssl rand -hex 32")
            : ValidateOptionsResult.Success;
    }
}
