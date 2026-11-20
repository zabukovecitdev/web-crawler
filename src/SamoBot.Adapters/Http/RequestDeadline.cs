namespace SamoBot.Adapters.Http;

internal sealed class RequestDeadline : IDisposable
{
    private readonly CancellationTokenSource _total;
    private readonly CancellationTokenSource _firstByte;
    private readonly CancellationTokenSource _linked;

    public RequestDeadline(TimeSpan total, TimeSpan firstByte, TimeProvider timeProvider, CancellationToken outer)
    {
        _total = new CancellationTokenSource(Normalize(total), timeProvider);
        _firstByte = new CancellationTokenSource(Normalize(firstByte), timeProvider);
        _linked = CancellationTokenSource.CreateLinkedTokenSource(outer, _total.Token, _firstByte.Token);
    }

    public CancellationToken Token => _linked.Token;

    public void HeadersReceived() => _firstByte.CancelAfter(Timeout.InfiniteTimeSpan);

    public static TimeSpan Seconds(int seconds) => seconds > 0 ? TimeSpan.FromSeconds(seconds) : Timeout.InfiniteTimeSpan;

    public static bool IsConnectTimeout(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is TimeoutException)
            {
                return true;
            }
        }

        return false;
    }

    public void Dispose()
    {
        _linked.Dispose();
        _firstByte.Dispose();
        _total.Dispose();
    }

    private static TimeSpan Normalize(TimeSpan value) => value > TimeSpan.Zero ? value : Timeout.InfiniteTimeSpan;
}
