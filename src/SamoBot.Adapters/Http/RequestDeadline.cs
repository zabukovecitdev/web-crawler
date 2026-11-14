namespace SamoBot.Adapters.Http;

/// <summary>
/// The time limits of one HTTP request. One total deadline covers everything, the body read included, and a shorter
/// first-byte deadline covers the time until the response headers arrive (call <see cref="HeadersReceived"/> then).
/// Use <see cref="Token"/> for the send and for the body read. When a deadline expires the token is cancelled.
/// A cancellation of the caller's own token also cancels <see cref="Token"/>, so the caller tells the two apart with
/// its own token (<c>!ct.IsCancellationRequested</c> means a timeout). A duration of zero or less turns that deadline off.
/// </summary>
internal sealed class RequestDeadline : IDisposable
{
    private readonly CancellationTokenSource _cts;
    private readonly ITimer _total;
    private readonly ITimer _firstByte;

    public RequestDeadline(TimeSpan total, TimeSpan firstByte, TimeProvider timeProvider, CancellationToken outer)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(outer);
        _total = timeProvider.CreateTimer(_ => Expire(), null, Normalize(total), Timeout.InfiniteTimeSpan);
        _firstByte = timeProvider.CreateTimer(_ => Expire(), null, Normalize(firstByte), Timeout.InfiniteTimeSpan);
    }

    public CancellationToken Token => _cts.Token;

    /// <summary>Switches the first-byte deadline off. The total deadline keeps running.</summary>
    public void HeadersReceived() => _firstByte.Dispose();

    public static TimeSpan Seconds(int seconds) => seconds > 0 ? TimeSpan.FromSeconds(seconds) : Timeout.InfiniteTimeSpan;

    /// <summary>True when the exception, or one of its inner exceptions, is the timeout of <see cref="SafeConnect"/>.</summary>
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
        _total.Dispose();
        _firstByte.Dispose();
        _cts.Dispose();
    }

    private static TimeSpan Normalize(TimeSpan value) => value > TimeSpan.Zero ? value : Timeout.InfiniteTimeSpan;

    private void Expire()
    {
        try
        {
            _cts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // The request finished while the timer fired.
        }
    }
}
