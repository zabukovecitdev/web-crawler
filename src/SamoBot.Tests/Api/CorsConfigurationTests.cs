using FluentAssertions;
using Microsoft.Extensions.Configuration;
using SamoBot.Api.Hosting;

namespace SamoBot.Tests.Api;

public class CorsConfigurationTests
{
    private static string SourceFile(string name)
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            var candidate = Path.Combine(directory.FullName, "src", "SamoBot.Api", name);
            if (File.Exists(candidate))
            {
                return candidate;
            }

            directory = directory.Parent;
        }

        throw new FileNotFoundException(name);
    }

    private static string[] Bind(IConfiguration configuration)
    {
        var options = new DashboardCorsOptions();
        configuration.GetSection(DashboardCorsOptions.SectionName).Bind(options);
        return options.Origins;
    }

    [Fact]
    public void AnEnvironmentOriginReplacesTheBaseFileInsteadOfMergingWithIt()
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile(SourceFile("appsettings.json"))
            .AddInMemoryCollection(new Dictionary<string, string?> { ["Cors:Origins:0"] = "https://dash.example" })
            .Build();

        Bind(configuration).Should().Equal("https://dash.example");
    }

    [Fact]
    public void ProductionConfigurationAllowsNoCrossOriginCaller()
    {
        var configuration = new ConfigurationBuilder().AddJsonFile(SourceFile("appsettings.json")).Build();

        Bind(configuration).Should().BeEmpty();
    }

    [Fact]
    public void DevelopmentConfigurationAllowsTheViteOrigins()
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile(SourceFile("appsettings.json"))
            .AddJsonFile(SourceFile("appsettings.Development.json"))
            .Build();

        Bind(configuration).Should().Equal("http://localhost:5173", "http://127.0.0.1:5173");
    }
}
