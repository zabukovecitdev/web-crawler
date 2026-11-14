namespace SamoBot.Api.Authentication;

public static class AuthSchemes
{
    /// <summary>Policy scheme: the API key when <c>X-Api-Key</c> is present, otherwise the cookie.</summary>
    public const string Smart = "Smart";

    public const string Cookie = "Cookies";

    public const string CookieName = "samobot.session";
}

public static class AuthRoutes
{
    public const string GoogleSignIn = "/api/v1/auth/google";
}
