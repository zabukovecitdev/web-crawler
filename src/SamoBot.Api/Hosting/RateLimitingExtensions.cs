using System.Security.Claims;
using System.Threading.RateLimiting;

namespace SamoBot.Api.Hosting;

/// <summary>
/// One fixed window per signed-in user (or per client address when anonymous) bounds how much
/// expensive read-model SQL a single caller can trigger. The live hub is not limited.
/// </summary>
public static class RateLimitingExtensions
{
    public const string PermitsKey = "RateLimiting:PermitsPerMinute";

    public const int DefaultPermitsPerMinute = 300;

    private const string HubPrefix = "/hubs";

    public static IServiceCollection AddSamoBotRateLimiting(this IServiceCollection services, IConfiguration configuration)
    {
        var permits = configuration.GetValue(PermitsKey, DefaultPermitsPerMinute);
        services.AddRateLimiter(options =>
        {
            options.RejectionStatusCode = StatusCodes.Status429TooManyRequests;
            options.OnRejected = (context, _) =>
            {
                context.HttpContext.Response.Headers.RetryAfter = "60";
                return ValueTask.CompletedTask;
            };
            options.GlobalLimiter = PartitionedRateLimiter.Create<HttpContext, string>(
                context => Partition(context, permits));
        });
        return services;
    }

    public static IApplicationBuilder UseSamoBotRateLimiting(this IApplicationBuilder app) => app.UseRateLimiter();

    private static RateLimitPartition<string> Partition(HttpContext context, int permits)
    {
        if (context.Request.Path.StartsWithSegments(HubPrefix, StringComparison.OrdinalIgnoreCase))
        {
            return RateLimitPartition.GetNoLimiter("hubs");
        }

        return RateLimitPartition.GetFixedWindowLimiter(KeyFor(context), _ => new FixedWindowRateLimiterOptions
        {
            PermitLimit = permits,
            Window = TimeSpan.FromMinutes(1),
            QueueLimit = 0
        });
    }

    private static string KeyFor(HttpContext context)
    {
        var user = context.User.FindFirstValue(ClaimTypes.NameIdentifier);
        return user is not null
            ? $"user:{user}"
            : $"ip:{context.Connection.RemoteIpAddress}";
    }
}
