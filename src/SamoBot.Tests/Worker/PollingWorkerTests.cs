using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Core.Common;
using SamoBot.Tests.Fakes;
using SamoBot.Worker;

namespace SamoBot.Tests.Worker;

public class PollingWorkerTests
{
    /// <summary>The test sets what each call does. The call number starts at 1.</summary>
    public sealed class Script
    {
        private int _calls;

        public Func<int, int> Behavior { get; set; } = _ => 0;

        public int Calls => Volatile.Read(ref _calls);

        public int Next() => Behavior(Interlocked.Increment(ref _calls));
    }

    public sealed class ScriptedHandler(Script script) : IWorkHandler
    {
        public Task<int> HandleBatch(CancellationToken ct) => Task.FromResult(script.Next());
    }

    private readonly FakeTimeProvider _time = new(TestData.Now);
    private readonly Script _script = new();

    private PollingWorker<ScriptedHandler> Worker()
    {
        var provider = new ServiceCollection()
            .AddSingleton(_script)
            .AddScoped<ScriptedHandler>()
            .BuildServiceProvider();
        return new PollingWorker<ScriptedHandler>(
            provider.GetRequiredService<IServiceScopeFactory>(),
            _time,
            NullLogger<PollingWorker<ScriptedHandler>>.Instance);
    }

    private static async Task Eventually(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition() && DateTime.UtcNow < deadline)
        {
            await Task.Delay(5);
        }

        condition().Should().BeTrue("the condition must hold within 5 seconds");
    }

    private async Task AdvanceUntil(Func<bool> condition, TimeSpan step)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition() && DateTime.UtcNow < deadline)
        {
            _time.Advance(step);
            await Task.Delay(5);
        }
    }

    [Fact]
    public async Task ExecuteAsyncWhenTheHandlerThrowsLogsAndTriesAgainAfterTheErrorDelay()
    {
        _script.Behavior = call => call == 1 ? throw new InvalidOperationException("boom") : 0;
        using var worker = Worker();

        await worker.StartAsync(default);
        await Eventually(() => _script.Calls == 1);
        await Task.Delay(50);
        _script.Calls.Should().Be(1, "the worker waits after an error");
        await AdvanceUntil(() => _script.Calls >= 2, PollingDelays.Error);
        await worker.StopAsync(default);

        _script.Calls.Should().BeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task ExecuteAsyncWhenABatchHasNoWorkWaitsTheIdleDelay()
    {
        using var worker = Worker();

        await worker.StartAsync(default);
        await Eventually(() => _script.Calls == 1);
        await Task.Delay(50);
        _script.Calls.Should().Be(1);
        await AdvanceUntil(() => _script.Calls >= 2, PollingDelays.Idle);
        await worker.StopAsync(default);

        _script.Calls.Should().BeGreaterThanOrEqualTo(2);
    }

    [Fact]
    public async Task ExecuteAsyncWhenABatchHasWorkRunsTheNextBatchWithoutWaiting()
    {
        _script.Behavior = _ => 5;
        using var worker = Worker();

        await worker.StartAsync(default);
        await Eventually(() => _script.Calls >= 20);
        await worker.StopAsync(default);

        _script.Calls.Should().BeGreaterThanOrEqualTo(20);
    }

    [Fact]
    public async Task StopAsyncEndsTheLoop()
    {
        using var worker = Worker();
        await worker.StartAsync(default);
        await Eventually(() => _script.Calls >= 1);

        await worker.StopAsync(default);

        worker.ExecuteTask!.IsCompleted.Should().BeTrue();
    }
}
