using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SamoBot.Adapters;
using SamoBot.Mcp.Tools;

var builder = Host.CreateApplicationBuilder(args);

// Configure all logs to go to stderr (stdout is used for the MCP protocol messages).
builder.Logging.AddConsole(o => o.LogToStandardErrorThreshold = LogLevel.Trace);

builder.Services.AddSamoBotSearch(builder.Configuration);

// Add the MCP services: the transport to use (stdio) and the tools to register.
builder.Services
    .AddMcpServer()
    .WithStdioServerTransport()
    .WithTools<SearchTools>();

await builder.Build().RunAsync();