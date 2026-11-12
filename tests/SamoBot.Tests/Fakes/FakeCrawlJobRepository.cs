using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeCrawlJobRepository : ICrawlJobRepository
{
    private readonly Dictionary<int, CrawlJob> _jobs = [];
    private int _nextId = 42;

    public List<CrawlJob> Saved { get; } = [];
    public IReadOnlyList<int> FinishedJobIds { get; set; } = [];

    public void Add(CrawlJob job) => _jobs[job.Id] = job;

    public Task<CrawlJob?> Get(int id, CancellationToken ct) => Task.FromResult(_jobs.GetValueOrDefault(id));

    public Task<IReadOnlyList<CrawlJob>> ListRecent(int limit, CancellationToken ct) =>
        Task.FromResult<IReadOnlyList<CrawlJob>>(_jobs.Values.Take(limit).ToList());

    public Task<int> Insert(CrawlJob job, CancellationToken ct)
    {
        var id = _nextId++;
        _jobs[id] = job;
        return Task.FromResult(id);
    }

    public Task Save(CrawlJob job, CancellationToken ct)
    {
        Saved.Add(job);
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<int>> CompleteFinishedJobs(DateTimeOffset now, CancellationToken ct) =>
        Task.FromResult(FinishedJobIds);
}
