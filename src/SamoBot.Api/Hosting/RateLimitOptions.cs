using System.Globalization;

namespace SamoBot.Api.Hosting;

public enum LegacyKeyUse
{
    NotSet,
    Used,
    Ignored
}

public sealed class RateLimitOptions
{
    public const string SectionName = "RateLimiting";

    public const string LegacyUserKeyName = "PermitsPerMinute";

    public int UserPermitsPerMinute { get; init; } = 300;

    public int AnonymousPermitsPerMinute { get; init; } = 120;

    public int SignInPermitsPerMinute { get; init; } = 10;

    public int ApiKeyFailuresPerMinute { get; init; } = 10;

    public LegacyKeyUse LegacyUserKey { get; private init; }

    public static RateLimitOptions From(IConfigurationSection section)
    {
        var defaults = new RateLimitOptions();
        var user = Read(section, nameof(UserPermitsPerMinute));
        var legacy = Read(section, LegacyUserKeyName);
        return new RateLimitOptions
        {
            UserPermitsPerMinute = user ?? legacy ?? defaults.UserPermitsPerMinute,
            AnonymousPermitsPerMinute = Read(section, nameof(AnonymousPermitsPerMinute)) ?? defaults.AnonymousPermitsPerMinute,
            SignInPermitsPerMinute = Read(section, nameof(SignInPermitsPerMinute)) ?? defaults.SignInPermitsPerMinute,
            ApiKeyFailuresPerMinute = Read(section, nameof(ApiKeyFailuresPerMinute)) ?? defaults.ApiKeyFailuresPerMinute,
            LegacyUserKey = UseOf(user, legacy)
        };
    }

    public void Validate()
    {
        Require(UserPermitsPerMinute, LegacyUserKey == LegacyKeyUse.Used ? $"{LegacyUserKeyName} (deprecated)" : nameof(UserPermitsPerMinute));
        Require(AnonymousPermitsPerMinute, nameof(AnonymousPermitsPerMinute));
        Require(SignInPermitsPerMinute, nameof(SignInPermitsPerMinute));
        Require(ApiKeyFailuresPerMinute, nameof(ApiKeyFailuresPerMinute));
    }

    private static LegacyKeyUse UseOf(int? user, int? legacy) => (user, legacy) switch
    {
        (_, null) => LegacyKeyUse.NotSet,
        (null, _) => LegacyKeyUse.Used,
        _ => LegacyKeyUse.Ignored
    };

    private static int? Read(IConfigurationSection section, string key)
    {
        var text = section[key];
        if (string.IsNullOrWhiteSpace(text))
        {
            return null;
        }

        return int.TryParse(text.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value)
            ? value
            : throw new InvalidOperationException($"{SectionName}:{key} must be a whole number.");
    }

    private static void Require(int value, string name)
    {
        if (value < 1)
        {
            throw new InvalidOperationException($"{SectionName}:{name} must be at least 1.");
        }
    }
}
