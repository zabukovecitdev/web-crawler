namespace SamoBot.Api.Authentication;

/// <summary>
/// Cookie sessions need CSRF protection on state-changing calls. A cross-site form cannot set a custom
/// header, and a cross-site fetch with one triggers a CORS preflight that only allowed origins pass.
/// Requests authenticated with the API key (not a cookie) are exempt.
/// </summary>
public sealed class CsrfHeaderMiddleware
{
    public const string HeaderName = "X-Requested-With";
    public const string HeaderValue = "samobot";

    private readonly RequestDelegate _next;

    public CsrfHeaderMiddleware(RequestDelegate next)
    {
        _next = next;
    }

    public Task InvokeAsync(HttpContext context)
    {
        if (!RequiresHeader(context) || HasHeader(context))
        {
            return _next(context);
        }

        context.Response.StatusCode = StatusCodes.Status403Forbidden;
        return context.Response.WriteAsJsonAsync(new
        {
            title = "Forbidden",
            status = StatusCodes.Status403Forbidden,
            detail = $"State-changing requests need the header {HeaderName}: {HeaderValue}."
        }, context.RequestAborted);
    }

    private static bool HasHeader(HttpContext context) =>
        context.Request.Headers[HeaderName] == HeaderValue;

    private static bool RequiresHeader(HttpContext context)
    {
        var method = context.Request.Method;
        var unsafeMethod = HttpMethods.IsPost(method) || HttpMethods.IsPut(method)
                           || HttpMethods.IsPatch(method) || HttpMethods.IsDelete(method);
        return unsafeMethod
               && !context.Request.Path.StartsWithSegments(AuthRoutes.GoogleSignIn, StringComparison.OrdinalIgnoreCase)
               && context.User.Identity?.AuthenticationType == AuthSchemes.Cookie;
    }
}
