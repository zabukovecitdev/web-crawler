using Microsoft.OpenApi.Models;
using SamoBot.Api.Authentication;
using SamoBot.Api.Hosting;
using SamoBot.Api.Hubs;
using SamoBot.Adapters;
using SamoBot.Adapters.Http;
using SamoBot.Core;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSamoBotCore();
builder.Services.AddSamoBotAdapters(builder.Configuration);

builder.Services.AddSamoBotAuth(builder.Configuration, builder.Environment);

builder.Services.AddSamoBotRateLimiting(builder.Configuration);

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(options =>
{
    options.SwaggerDoc("v1", new OpenApiInfo { Title = "SamoBot Dashboard API", Version = "v1" });
    options.AddSecurityDefinition("ApiKey", new OpenApiSecurityScheme
    {
        Description = "Send header `X-Api-Key` with the configured dashboard key.",
        Name = "X-Api-Key",
        In = ParameterLocation.Header,
        Type = SecuritySchemeType.ApiKey
    });
    options.AddSecurityRequirement(new OpenApiSecurityRequirement
    {
        {
            new OpenApiSecurityScheme
            {
                Reference = new OpenApiReference { Type = ReferenceType.SecurityScheme, Id = "ApiKey" }
            },
            Array.Empty<string>()
        }
    });
});

builder.Services.AddSignalR();
builder.Services.AddHostedService<CrawlEventListener>();

var corsOptions = new DashboardCorsOptions();
builder.Configuration.GetSection(DashboardCorsOptions.SectionName).Bind(corsOptions);
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.WithOrigins(corsOptions.Origins)
            .AllowAnyHeader()
            .AllowAnyMethod()
            .AllowCredentials()
            .WithExposedHeaders("X-Content-Truncated");
    });
});

var app = builder.Build();
app.Services.LogIfPrivateAddressesAllowed();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(options => options.SwaggerEndpoint("/swagger/v1/swagger.json", "SamoBot v1"));
}

app.UseCors();
app.UseSamoBotAuth();
app.UseSamoBotRateLimiting();

app.MapControllers();
app.MapHub<CrawlJobHub>("/hubs/crawl-job");

await app.RunAsync();
