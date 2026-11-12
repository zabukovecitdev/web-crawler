using Microsoft.AspNetCore.SignalR;
using SamoBot.Api.Hubs;

namespace SamoBot.Tests.Fakes;

/// <summary>A hub context that records the messages that are sent to groups. Each argument is stored as JSON text.</summary>
public sealed class RecordingHubContext : IHubContext<CrawlJobHub>
{
    private readonly List<(string Group, string Method, object?[] Args)> _sent = [];

    public IReadOnlyList<(string Group, string Method, object?[] Args)> Sent
    {
        get
        {
            lock (_sent)
            {
                return _sent.ToList();
            }
        }
    }

    public IHubClients Clients => new RecordingClients(Record);

    public IGroupManager Groups => throw new NotSupportedException();

    private void Record(string group, string method, object?[] args)
    {
        lock (_sent)
        {
            _sent.Add((group, method, args));
        }
    }

    private sealed class RecordingClients(Action<string, string, object?[]> record) : IHubClients
    {
        public IClientProxy All => throw new NotSupportedException();

        public IClientProxy Group(string groupName) => new RecordingProxy(groupName, record);

        public IClientProxy AllExcept(IReadOnlyList<string> excludedConnectionIds) => throw new NotSupportedException();

        public IClientProxy Client(string connectionId) => throw new NotSupportedException();

        public IClientProxy Clients(IReadOnlyList<string> connectionIds) => throw new NotSupportedException();

        public IClientProxy GroupExcept(string groupName, IReadOnlyList<string> excludedConnectionIds) => throw new NotSupportedException();

        public IClientProxy Groups(IReadOnlyList<string> groupNames) => throw new NotSupportedException();

        public IClientProxy User(string userId) => throw new NotSupportedException();

        public IClientProxy Users(IReadOnlyList<string> userIds) => throw new NotSupportedException();
    }

    private sealed class RecordingProxy(string group, Action<string, string, object?[]> record) : IClientProxy
    {
        public Task SendCoreAsync(string method, object?[] args, CancellationToken cancellationToken = default)
        {
            record(group, method, args.Select(arg => (object?)System.Text.Json.JsonSerializer.Serialize(arg)).ToArray());
            return Task.CompletedTask;
        }
    }
}
