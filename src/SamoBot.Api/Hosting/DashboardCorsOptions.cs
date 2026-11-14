namespace SamoBot.Api.Hosting;

/// <summary>
/// Browser origins that may call the API with credentials. Empty means no cross-origin caller.
/// The development list lives in appsettings.Development.json so that it never merges into an
/// environment override (array configuration merges by index).
/// </summary>
public sealed class DashboardCorsOptions
{
    public const string SectionName = "Cors";

    public string[] Origins { get; set; } = [];
}
