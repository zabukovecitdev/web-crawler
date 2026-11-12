using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SamoBot.Adapters;
using SamoBot.Core;
using SamoBot.Core.Common;
using SamoBot.Core.Crawling;
using SamoBot.Core.Indexing;
using SamoBot.Core.Jobs;
using SamoBot.Core.Parsing;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Composition;

public class ServiceRegistrationTests
{
    private static ServiceProvider Build()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Database:ConnectionString"] = "Host=localhost;Database=none;Username=none;Password=none"
            })
            .Build();

        return new ServiceCollection()
            .AddLogging()
            .AddSamoBotCore()
            .AddSamoBotAdapters(configuration)
            .BuildServiceProvider(new ServiceProviderOptions { ValidateScopes = true, ValidateOnBuild = true });
    }

    [Theory]
    [InlineData(typeof(CrawlBatchHandler))]
    [InlineData(typeof(ParseBatchHandler))]
    [InlineData(typeof(IndexBatchHandler))]
    [InlineData(typeof(JobCompletionHandler))]
    [InlineData(typeof(CrawlJobService))]
    public async Task Resolve_BuildsEachHandlerAndServiceWithAllItsDependencies(Type type)
    {
        await using var provider = Build();
        await using var scope = provider.CreateAsyncScope();

        scope.ServiceProvider.GetRequiredService(type).Should().NotBeNull();
        if (type != typeof(CrawlJobService))
        {
            scope.ServiceProvider.GetRequiredService(type).Should().BeAssignableTo<IWorkHandler>();
        }
    }

    [Fact]
    public async Task Resolve_GivesExactlyOneSupportingFetcherForEveryUrl()
    {
        await using var provider = Build();
        await using var scope = provider.CreateAsyncScope();
        var fetchers = scope.ServiceProvider.GetServices<IFetcher>().ToList();

        fetchers.Should().HaveCount(2);
        foreach (var useJs in new[] { false, true })
        {
            var url = Fakes.TestData.Url(useJsRendering: useJs);
            fetchers.Count(fetcher => fetcher.Supports(url)).Should().Be(1);
        }
    }

    [Fact]
    public async Task Resolve_ReusesTheSameMeilisearchIndexForReadingAndWriting()
    {
        await using var provider = Build();

        provider.GetRequiredService<ISearchIndex>().Should().BeSameAs(provider.GetRequiredService<ISearchQuery>());
        provider.GetRequiredService<ILoggerFactory>().Should().NotBeNull();
    }

    [Fact]
    public async Task Resolve_BuildsTheFetcherResolverThatTheCrawlerUses()
    {
        await using var provider = Build();
        await using var scope = provider.CreateAsyncScope();

        var resolver = scope.ServiceProvider.GetRequiredService<FetcherResolver>();

        resolver.For(Fakes.TestData.Url()).Should().NotBeNull();
    }
}
