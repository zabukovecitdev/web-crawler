using FluentAssertions;
using Meilisearch;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SamoBot.Adapters.Search;
using SamoBot.Tests.EndToEnd;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Search;

[Trait("Category", "Integration")]
public sealed class RetiredIndexCleanupTests(InfrastructureFixture infrastructure) : IClassFixture<InfrastructureFixture>, IDisposable
{
    private const string Retired = "parsed_documents";

    private readonly MeilisearchClient _client = new(infrastructure.MeilisearchUrl, InfrastructureFixture.MeilisearchKey);
    private readonly CapturingLoggerProvider _logs = new();

    public void Dispose() => _logs.Dispose();

    private RetiredIndexCleanup Cleanup(string indexName = "pages") =>
        new(_client, Options.Create(new MeilisearchOptions { IndexName = indexName }), new LoggerFactory([_logs]).CreateLogger<RetiredIndexCleanup>());

    private async Task Wait(TaskInfo task) => await _client.WaitForTaskAsync(task.TaskUid);

    private async Task<bool> Exists(string index)
    {
        try
        {
            await _client.GetIndexAsync(index);
            return true;
        }
        catch (MeilisearchApiError error) when (error.Code == "index_not_found")
        {
            return false;
        }
    }

    [Fact]
    public async Task RunDeletesTheRetiredIndexAndSaysSo()
    {
        await Wait(await _client.CreateIndexAsync(Retired));

        await Cleanup().Run(default);

        (await Exists(Retired)).Should().BeFalse();
        _logs.Messages.Should().ContainSingle().Which.Should().Contain("Deleted the retired search index parsed_documents");
    }

    [Fact]
    public async Task RunIsQuietWhenTheRetiredIndexIsAlreadyGone()
    {
        await Wait(await _client.DeleteIndexAsync(Retired));

        await Cleanup().Run(default);

        _logs.Messages.Should().BeEmpty();
    }

    [Fact]
    public async Task RunNeverDeletesTheIndexInUse()
    {
        await Wait(await _client.CreateIndexAsync(Retired));

        await Cleanup(indexName: Retired).Run(default);

        (await Exists(Retired)).Should().BeTrue();
        await Wait(await _client.DeleteIndexAsync(Retired));
    }

    [Fact]
    public async Task RunLogsInsteadOfThrowingWhenMeilisearchIsUnreachable()
    {
        var unreachable = new RetiredIndexCleanup(
            new MeilisearchClient("http://127.0.0.1:9", InfrastructureFixture.MeilisearchKey),
            Options.Create(new MeilisearchOptions()),
            new LoggerFactory([_logs]).CreateLogger<RetiredIndexCleanup>());

        var run = () => unreachable.Run(default);

        await run.Should().NotThrowAsync();
        _logs.Messages.Should().ContainSingle().Which.Should().Contain("Could not delete the retired search index");
    }
}
