using FluentAssertions;
using Meilisearch;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SamoBot.Adapters.Search;
using SamoBot.Tests.EndToEnd;

namespace SamoBot.Tests.Search;

[Trait("Category", "Integration")]
public class MeilisearchSettingsTests(InfrastructureFixture infrastructure) : IClassFixture<InfrastructureFixture>
{
    private MeilisearchClient Client() => new(infrastructure.MeilisearchUrl, InfrastructureFixture.MeilisearchKey);

    private static MeilisearchSettings Settings(MeilisearchClient client) => new(
        client,
        Options.Create(new MeilisearchOptions { IndexName = $"pages_{Guid.NewGuid():N}" }),
        NullLogger<MeilisearchSettings>.Instance);

    [Fact]
    public async Task EnsureDeletesTheRetiredParsedDocumentsIndex()
    {
        var client = Client();
        var created = await client.CreateIndexAsync("parsed_documents");
        await client.WaitForTaskAsync(created.TaskUid);
        using var settings = Settings(client);

        await settings.Ensure(default);

        var lookup = () => client.GetIndexAsync("parsed_documents");
        (await lookup.Should().ThrowAsync<MeilisearchApiError>()).Which.Code.Should().Be("index_not_found");
    }

    [Fact]
    public async Task EnsureSucceedsWhenTheRetiredIndexIsAlreadyGone()
    {
        using var settings = Settings(Client());

        var ensure = () => settings.Ensure(default);

        await ensure.Should().NotThrowAsync();
    }
}
