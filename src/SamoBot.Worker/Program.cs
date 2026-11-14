using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using SamoBot.Adapters;
using SamoBot.Adapters.Http;
using SamoBot.Core;
using SamoBot.Core.Crawling;
using SamoBot.Core.Indexing;
using SamoBot.Core.Jobs;
using SamoBot.Core.Parsing;
using SamoBot.Worker;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSamoBotCore();
builder.Services.AddSamoBotAdapters(builder.Configuration);

builder.Services.AddHostedService<PollingWorker<CrawlBatchHandler>>();
builder.Services.AddHostedService<PollingWorker<ParseBatchHandler>>();
builder.Services.AddHostedService<PollingWorker<IndexBatchHandler>>();
builder.Services.AddHostedService<PollingWorker<JobCompletionHandler>>();

var host = builder.Build();
host.Services.LogIfPrivateAddressesAllowed();
await host.RunAsync();
