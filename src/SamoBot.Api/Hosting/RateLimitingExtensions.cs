using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Security.Claims;
using System.Threading.RateLimiting;
using Microsoft.AspNetCore.RateLimiting;
using SamoBot.Api.Authentication;

namespace SamoBot.Api.Hosting;

/// <summary>
/// The API's rate limiter, and the only one: the dashboard's nginx does not limit. It keys on the signed-in
/// user, otherwise on the client address, and counts a wrong <c>X-Api-Key</c> separately with far fewer
/// attempts. All limiters are token buckets: a short burst is allowed, then requests are admitted at a steady
/// rate. It runs after authentication and before authorization, so unauthenticated calls to protected
/// endpoints (a 401) are counted too. The hub is limited like any path, which only counts the HTTP requests
/// that open a connection (negotiate, upgrade), not the messages inside it.
/// </summary>
public static partial class RateLimitingExtensions
{
    public const string SignInPolicy = "auth-signin";

    private const int MinimumBurst = 3;

    private const int FallbackRetryAfterSeconds = 60;

    public static IServiceCollection AddSamoBotRateLimiting(this IServiceCollection services, IConfiguration configuration)
    {
        var limits = configuration.GetSection(RateLimitOptions.SectionName).Get<RateLimitOptions>() ?? new RateLimitOptions();
        limits.Validate();

        services.AddRateLimiter(options =>
        {
            options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
            options.OnRejected = WriteRejectionAsync;
            options.GlobalLimiter = PartitionedRateLimiter.Create<HttpContext, string>(context => Partition(context, limits));
            options.AddPolicy<string>(SignInPolicy, context =>
                RateLimitPartition.GetTokenBucketLimiter(
                    $"signin:{ClientKey(context)}", _ => Bucket(limits.SignInPermitsPerMinute)));
        });
        return services;
    }

    public static IApplicationBuilder UseSamoBotRateLimiting(this IApplicationBuilder app) => app.UseRateLimiter();

    private static RateLimitPartition<string> Partition(HttpContext context, RateLimitOptions limits)
    {
        var signedIn = context.User.Identity?.IsAuthenticated == true;
        var user = signedIn ? context.User.FindFirstValue(ClaimTypes.NameIdentifier) : null;
        if (user is not null)
        {
            return Token($"user:{user}", limits.PermitsPerMinute);
        }

        // An X-Api-Key that did not authenticate is a wrong guess: give those far fewer attempts.
        return context.Request.Headers.ContainsKey(ApiKeyAuthenticationHandler.HeaderName)
            ? Token($"apikey-failure:{ClientKey(context)}", limits.ApiKeyFailuresPerMinute)
            : Token($"anonymous:{ClientKey(context)}", limits.AnonymousPermitsPerMinute);
    }

    private static RateLimitPartition<string> Token(string key, int permitsPerMinute) =>
        RateLimitPartition.GetTokenBucketLimiter(key, _ => Bucket(permitsPerMinute));

    private static TokenBucketRateLimiterOptions Bucket(int permitsPerMinute) => new()
    {
        TokenLimit = Math.Max(MinimumBurst, permitsPerMinute / 5),
        TokensPerPeriod = 1,
        ReplenishmentPeriod = TimeSpan.FromSeconds(60d / permitsPerMinute),
        QueueLimit = 0,
        AutoReplenishment = true
    };

    /// <summary>The client address; an IPv6 client is limited as a whole /64, since one host owns that many.</summary>
    internal static string ClientKey(HttpContext context)
    {
        var address = context.Connection.RemoteIpAddress;
        if (address is null)
        {
            return "unknown";
        }

        if (address.IsIPv4MappedToIPv6)
        {
            address = address.MapToIPv4();
        }

        if (address.AddressFamily != AddressFamily.InterNetworkV6)
        {
            return address.ToString();
        }

        var bytes = address.GetAddressBytes();
        Array.Clear(bytes, 8, bytes.Length - 8);
        return $"{new IPAddress(bytes)}/64";
    }

    private static async ValueTask WriteRejectionAsync(OnRejectedContext context, CancellationToken cancellationToken)
    {
        var retryAfter = context.Lease.TryGetMetadata(MetadataName.RetryAfter, out var delay)
            ? Math.Max(1, (int)Math.Ceiling(delay.TotalSeconds))
            : FallbackRetryAfterSeconds;
        var http = context.HttpContext;
        http.Response.Headers.RetryAfter = retryAfter.ToString(CultureInfo.InvariantCulture);
        LogRejected(http.RequestServices.GetRequiredService<ILoggerFactory>().CreateLogger("SamoBot.Api.RateLimiting"), http.Request.Path.Value ?? string.Empty);
        await http.Response.WriteAsJsonAsync(new
        {
            title = "Too many requests",
            status = StatusCodes.Status429TooManyRequests,
            detail = $"Slow down and retry in {retryAfter} seconds."
        }, cancellationToken);
    }

    [LoggerMessage(EventId = 1, Level = LogLevel.Warning, Message = "Rate limit exceeded for {Path}.")]
    private static partial void LogRejected(ILogger logger, string path);
}

/// <summary>
/// Requests per minute for each kind of caller. Section <c>RateLimiting</c>; the per-user key
/// <c>PermitsPerMinute</c> keeps its old name.
/// </summary>
public sealed class RateLimitOptions
{
    public const string SectionName = "RateLimiting";

    /// <summary>A signed-in user (cookie session or API key).</summary>
    public int PermitsPerMinute { get; set; } = 300;

    /// <summary>One client address without credentials.</summary>
    public int AnonymousPermitsPerMinute { get; set; } = 120;

    /// <summary>One client address on <c>POST /api/v1/auth/google</c>, on top of the anonymous limit.</summary>
    public int SignInPermitsPerMinute { get; set; } = 10;

    /// <summary>One client address sending an <c>X-Api-Key</c> that does not match.</summary>
    public int ApiKeyFailuresPerMinute { get; set; } = 10;

    public void Validate()
    {
        Require(PermitsPerMinute, nameof(PermitsPerMinute));
        Require(AnonymousPermitsPerMinute, nameof(AnonymousPermitsPerMinute));
        Require(SignInPermitsPerMinute, nameof(SignInPermitsPerMinute));
        Require(ApiKeyFailuresPerMinute, nameof(ApiKeyFailuresPerMinute));
    }

    private static void Require(int value, string name)
    {
        if (value < 1)
        {
            throw new InvalidOperationException($"{SectionName}:{name} must be at least 1.");
        }
    }
}
