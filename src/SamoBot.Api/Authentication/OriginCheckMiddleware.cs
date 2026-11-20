using Microsoft.Extensions.Options;
using SamoBot.Api.Hosting;

namespace SamoBot.Api.Authentication;

public sealed class OriginCheckMiddleware
{
    private const string HubPrefix = "/hubs";

    private readonly RequestDelegate _next;
    private readonly IOptions<DashboardCorsOptions> _cors;

    public OriginCheckMiddleware(RequestDelegate next, IOptions<DashboardCorsOptions> cors)
    {
        _next = next;
        _cors = cors;
    }

    public Task InvokeAsync(HttpContext context)
    {
        if (!IsChecked(context) || IsAllowed(context))
        {
            return _next(context);
        }

        context.Response.StatusCode = StatusCodes.Status403Forbidden;
        return context.Response.WriteAsJsonAsync(new
        {
            title = "Forbidden",
            status = StatusCodes.Status403Forbidden,
            detail = "Origin not allowed."
        }, context.RequestAborted);
    }

    private static bool IsChecked(HttpContext context)
    {
        var path = context.Request.Path;
        return path.StartsWithSegments(HubPrefix, StringComparison.OrdinalIgnoreCase)
               || (HttpMethods.IsPost(context.Request.Method)
                   && path.StartsWithSegments(AuthRoutes.GoogleSignIn, StringComparison.OrdinalIgnoreCase));
    }

    private bool IsAllowed(HttpContext context)
    {
        if (!context.Request.Headers.TryGetValue("Origin", out var values))
        {
            return true;
        }

        var origin = values.ToString().TrimEnd('/');
        var own = $"{context.Request.Scheme}://{context.Request.Host}";
        return string.Equals(origin, own, StringComparison.OrdinalIgnoreCase)
               || _cors.Value.Origins.Any(o => string.Equals(o.TrimEnd('/'), origin, StringComparison.OrdinalIgnoreCase));
    }
}
