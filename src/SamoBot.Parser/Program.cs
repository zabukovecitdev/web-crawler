using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SamoBot.Infrastructure.Extensions;
using SamoBot.Infrastructure.Storage.Services;
using SamoBot.Parser.Extensions;
using SamoBot.Workers;

var builder = Host.CreateApplicationBuilder(args);

builder.Configuration
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .AddJsonFile($"appsettings.{builder.Environment.EnvironmentName}.json", optional: true, reloadOnChange: true)
    .AddEnvironmentVariables();

builder.Services.AddDatabase(builder.Configuration);
builder.Services.AddRedisCache(builder.Configuration);
builder.Services.AddRabbitMq(builder.Configuration);
builder.Services.AddMinioStorage(builder.Configuration);
builder.Services.AddCrawling(builder.Configuration);
builder.Services.AddNeo4j(builder.Configuration);
builder.Services.AddHostedService<MinioBucketInitializationService>();
builder.Services.AddParserServices();
builder.Services.AddHostedService<ParserWorker>();
builder.Services.AddHostedService<RobotsTxtRefreshWorker>();

builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.AddDebug();

var host = builder.Build();
await host.RunAsync();
