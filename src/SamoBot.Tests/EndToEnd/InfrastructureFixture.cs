using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace SamoBot.Tests.EndToEnd;

/// <summary>RustFS (S3 storage) and Meilisearch for the end-to-end test. PostgreSQL comes from the shared Postgres fixture.</summary>
public sealed class InfrastructureFixture : IAsyncLifetime
{
    public const string MeilisearchKey = "masterKey";
    public const string StorageUser = "minioadmin";

    private readonly IContainer _storage = new ContainerBuilder()
        .WithImage("rustfs/rustfs:1.0.5")
        .WithPortBinding(9000, true)
        .WithEnvironment("RUSTFS_ACCESS_KEY", StorageUser)
        .WithEnvironment("RUSTFS_SECRET_KEY", StorageUser)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request => request.ForPort(9000).ForPath("/health")))
        .Build();

    private readonly IContainer _meilisearch = new ContainerBuilder()
        .WithImage("getmeili/meilisearch:v1.11")
        .WithPortBinding(7700, true)
        .WithEnvironment("MEILI_MASTER_KEY", MeilisearchKey)
        .WithEnvironment("MEILI_ENV", "development")
        .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request => request.ForPort(7700).ForPath("/health")))
        .Build();

    public string StorageHost => _storage.Hostname;

    public int StoragePort => _storage.GetMappedPublicPort(9000);

    public string MeilisearchUrl => $"http://{_meilisearch.Hostname}:{_meilisearch.GetMappedPublicPort(7700)}";

    public async Task InitializeAsync() => await Task.WhenAll(_storage.StartAsync(), _meilisearch.StartAsync());

    public async Task DisposeAsync()
    {
        await _storage.DisposeAsync();
        await _meilisearch.DisposeAsync();
    }
}
