using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace SamoBot.Tests.EndToEnd;

/// <summary>MinIO and Meilisearch for the end-to-end test. PostgreSQL comes from the shared Postgres fixture.</summary>
public sealed class InfrastructureFixture : IAsyncLifetime
{
    public const string MeilisearchKey = "masterKey";
    public const string MinioUser = "minioadmin";

    private readonly IContainer _minio = new ContainerBuilder()
        .WithImage("bitnamilegacy/minio:latest")
        .WithPortBinding(9000, true)
        .WithEnvironment("MINIO_ROOT_USER", MinioUser)
        .WithEnvironment("MINIO_ROOT_PASSWORD", MinioUser)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request => request.ForPort(9000).ForPath("/minio/health/live")))
        .Build();

    private readonly IContainer _meilisearch = new ContainerBuilder()
        .WithImage("getmeili/meilisearch:v1.11")
        .WithPortBinding(7700, true)
        .WithEnvironment("MEILI_MASTER_KEY", MeilisearchKey)
        .WithEnvironment("MEILI_ENV", "development")
        .WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request => request.ForPort(7700).ForPath("/health")))
        .Build();

    public string MinioHost => _minio.Hostname;

    public int MinioPort => _minio.GetMappedPublicPort(9000);

    public string MeilisearchUrl => $"http://{_meilisearch.Hostname}:{_meilisearch.GetMappedPublicPort(7700)}";

    public async Task InitializeAsync() => await Task.WhenAll(_minio.StartAsync(), _meilisearch.StartAsync());

    public async Task DisposeAsync()
    {
        await _minio.DisposeAsync();
        await _meilisearch.DisposeAsync();
    }
}
