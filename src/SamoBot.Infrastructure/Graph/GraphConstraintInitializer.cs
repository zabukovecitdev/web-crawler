using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Neo4j.Driver;

namespace SamoBot.Infrastructure.Graph;

public class GraphConstraintInitializer : IHostedService
{
    private readonly IDriver? _driver;
    private readonly ILogger<GraphConstraintInitializer> _logger;

    public GraphConstraintInitializer(IDriver? driver, ILogger<GraphConstraintInitializer> logger)
    {
        _driver = driver;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_driver is null)
        {
            _logger.LogDebug("Neo4j driver is null, skipping constraint initialization");
            return;
        }

        try
        {
            await using var session = _driver.AsyncSession();
            await session.RunAsync(
                "CREATE CONSTRAINT page_url_unique IF NOT EXISTS FOR (p:Page) REQUIRE p.url IS UNIQUE");
            await session.RunAsync(
                "CREATE INDEX page_host IF NOT EXISTS FOR (p:Page) ON (p.host)");
            _logger.LogInformation("Neo4j constraints and indexes ensured");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to create Neo4j constraints/indexes. Graph writes may still work");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
