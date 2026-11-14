namespace SamoBot.Core.Domain;

/// <summary>
/// The new pacing state of a host after a fetch, stored with the fetch result in one transaction.
/// <see cref="Delay"/> is the wait until the next request to the host (never below the base delay of the host).
/// </summary>
public sealed record HostPacingUpdate(TimeSpan Delay, double? AvgResponseMs, int ConsecutiveFailures);
