using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IAnalyticsReader
{
    Task<AnalyticsOverview> Overview(AnalyticsRange range, CancellationToken ct);

    Task<IReadOnlyList<HostAnalytics>> Hosts(AnalyticsRange range, CancellationToken ct);

    Task<StatusClassCounts> StatusCodes(AnalyticsRange range, string? host, CancellationToken ct);

    Task<IReadOnlyList<AnalyticsContentType>> ContentTypes(AnalyticsRange range, string? host, CancellationToken ct);

    Task<IReadOnlyList<TimeseriesPoint>> Timeseries(AnalyticsRange range, string? host, AnalyticsBucket bucket, CancellationToken ct);
}
