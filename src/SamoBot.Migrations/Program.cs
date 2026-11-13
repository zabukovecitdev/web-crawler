using FluentMigrator.Runner;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using SamoBot.Migrations;

var builder = Host.CreateApplicationBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("DefaultConnection")
    ?? builder.Configuration["Database:ConnectionString"]
    ?? throw new InvalidOperationException("Database connection string is not configured");

builder.Services
    .AddFluentMigratorCore()
    .ConfigureRunner(rb => rb
        .AddPostgres()
        .WithGlobalConnectionString(connectionString)
        .ScanIn(typeof(DatabaseCreator).Assembly).For.Migrations())
    .AddLogging(lb => lb.AddFluentMigratorConsole());

using var host = builder.Build();

await DatabaseCreator.EnsureExists(connectionString);

using var scope = host.Services.CreateScope();
scope.ServiceProvider.GetRequiredService<IMigrationRunner>().MigrateUp();
Console.WriteLine("Database migrations completed.");
