using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;

namespace SamoBot.Api.Authentication;

public static class AuthServiceCollectionExtensions
{
    public static IServiceCollection AddSamoBotAuth(
        this IServiceCollection services, IConfiguration configuration, IHostEnvironment environment)
    {
        services.Configure<ApiKeyOptions>(configuration.GetSection(ApiKeyOptions.SectionName));
        services.Configure<AuthOptions>(configuration.GetSection(AuthOptions.SectionName));
        services.Configure<GoogleAuthOptions>(configuration.GetSection(GoogleAuthOptions.SectionName));
        services.AddSingleton<EmailAllowlist>();
        services.AddSingleton<IGoogleTokenValidator, GoogleTokenValidator>();
        services.AddHostedService<AllowlistStartupCheck>();

        services.AddAuthentication(AuthSchemes.Smart)
            .AddPolicyScheme(AuthSchemes.Smart, null, options =>
                options.ForwardDefaultSelector = SelectScheme)
            .AddScheme<AuthenticationSchemeOptions, ApiKeyAuthenticationHandler>(
                ApiKeyAuthenticationHandler.SchemeName, _ => { })
            .AddCookie(AuthSchemes.Cookie, options => ConfigureCookie(options, environment));
        services.AddAuthorization();
        return services;
    }

    public static IApplicationBuilder UseSamoBotAuth(this IApplicationBuilder app)
    {
        app.UseAuthentication();
        app.UseMiddleware<CsrfHeaderMiddleware>();
        app.UseAuthorization();
        return app;
    }

    private static string SelectScheme(HttpContext context) =>
        context.Request.Headers.ContainsKey(ApiKeyAuthenticationHandler.HeaderName)
            ? ApiKeyAuthenticationHandler.SchemeName
            : AuthSchemes.Cookie;

    private static void ConfigureCookie(CookieAuthenticationOptions options, IHostEnvironment environment)
    {
        options.Cookie.Name = AuthSchemes.CookieName;
        options.Cookie.HttpOnly = true;
        options.Cookie.SameSite = SameSiteMode.Lax;
        options.Cookie.SecurePolicy = environment.IsDevelopment()
            ? CookieSecurePolicy.SameAsRequest
            : CookieSecurePolicy.Always;
        options.ExpireTimeSpan = TimeSpan.FromHours(8);
        options.SlidingExpiration = true;
        options.Events.OnRedirectToLogin = context => RespondWith(context, StatusCodes.Status401Unauthorized);
        options.Events.OnRedirectToAccessDenied = context => RespondWith(context, StatusCodes.Status403Forbidden);
    }

    private static Task RespondWith(RedirectContext<CookieAuthenticationOptions> context, int status)
    {
        context.Response.StatusCode = status;
        return Task.CompletedTask;
    }
}
