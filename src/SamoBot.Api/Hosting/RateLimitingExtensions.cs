using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Security.Claims;
using System.Threading.RateLimiting;
using Microsoft.AspNetCore.RateLimiting;
using SamoBot.Api.Authentication;

namespace SamoBot.Api.Hosting;

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

public sealed class RateLimitOptions
{
    public const string SectionName = "RateLimiting";

    public int PermitsPerMinute { get; set; } = 300;

    public int AnonymousPermitsPerMinute { get; set; } = 120;

    public int SignInPermitsPerMinute { get; set; } = 10;

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
