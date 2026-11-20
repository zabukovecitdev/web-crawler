namespace SamoBot.Api.Hosting;

public sealed class DashboardCorsOptions
{
    public const string SectionName = "Cors";

    public string[] Origins { get; set; } = [];
}
