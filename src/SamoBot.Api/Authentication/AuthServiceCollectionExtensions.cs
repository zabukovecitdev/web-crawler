using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.Extensions.DependencyInjection.Extensions;
using SamoBot.Api.Hosting;
using Microsoft.Extensions.Options;

namespace SamoBot.Api.Authentication;

public static class AuthServiceCollectionExtensions
{
    public static IServiceCollection AddSamoBotAuth(
        this IServiceCollection services, IConfiguration configuration, IHostEnvironment environment)
    {
        services.AddOptions<ApiKeyOptions>()
            .Bind(configuration.GetSection(ApiKeyOptions.SectionName))
            .ValidateOnStart();
        services.AddSingleton<IValidateOptions<ApiKeyOptions>, ApiKeyOptionsValidator>();
        services.Configure<DashboardCorsOptions>(configuration.GetSection(DashboardCorsOptions.SectionName));
        services.Configure<AuthOptions>(configuration.GetSection(AuthOptions.SectionName));
        services.Configure<GoogleAuthOptions>(configuration.GetSection(GoogleAuthOptions.SectionName));
        services.TryAddSingleton(TimeProvider.System);
        services.AddSingleton<EmailAllowlist>();
        services.AddSingleton<IGoogleTokenValidator, GoogleTokenValidator>();
        services.AddHostedService<AllowlistStartupCheck>();

        services.AddAuthentication(AuthSchemes.Smart)
            .AddPolicyScheme(AuthSchemes.Smart, null, options =>
                options.ForwardDefaultSelector = SelectScheme)
            .AddScheme<AuthenticationSchemeOptions, ApiKeyAuthenticationHandler>(
                ApiKeyAuthenticationHandler.SchemeName, _ => { })
            .AddCookie(AuthSchemes.Cookie, options => ConfigureCookie(options, SecurePolicy(configuration, environment)));
        services.AddAuthorization();
        return services;
    }

    public static IApplicationBuilder UseSamoBotAuth(this IApplicationBuilder app)
    {
        app.UseAuthentication();
        app.UseMiddleware<OriginCheckMiddleware>();
        app.UseMiddleware<CsrfHeaderMiddleware>();
        app.UseAuthorization();
        return app;
    }

    private static string SelectScheme(HttpContext context) =>
        context.Request.Headers.ContainsKey(ApiKeyAuthenticationHandler.HeaderName)
            ? ApiKeyAuthenticationHandler.SchemeName
            : AuthSchemes.Cookie;

    /// <summary>Development, or an explicit <c>Auth:AllowInsecureCookie</c>, allows the cookie over plain http.</summary>
    private static CookieSecurePolicy SecurePolicy(IConfiguration configuration, IHostEnvironment environment)
    {
        var allowInsecure = configuration.GetValue<bool>($"{AuthOptions.SectionName}:{nameof(AuthOptions.AllowInsecureCookie)}");
        return environment.IsDevelopment() || allowInsecure
            ? CookieSecurePolicy.SameAsRequest
            : CookieSecurePolicy.Always;
    }

    private static void ConfigureCookie(CookieAuthenticationOptions options, CookieSecurePolicy securePolicy)
    {
        options.Cookie.Name = AuthSchemes.CookieName;
        options.Cookie.HttpOnly = true;
        options.Cookie.SameSite = SameSiteMode.Lax;
        options.Cookie.SecurePolicy = securePolicy;
        options.ExpireTimeSpan = TimeSpan.FromHours(8);
        options.SlidingExpiration = true;
        options.Events.OnValidatePrincipal = SessionValidation.ValidateAsync;
        options.Events.OnRedirectToLogin = context => RespondWith(context, StatusCodes.Status401Unauthorized);
        options.Events.OnRedirectToAccessDenied = context => RespondWith(context, StatusCodes.Status403Forbidden);
    }

    private static Task RespondWith(RedirectContext<CookieAuthenticationOptions> context, int status)
    {
        context.Response.StatusCode = status;
        return Task.CompletedTask;
    }
}
