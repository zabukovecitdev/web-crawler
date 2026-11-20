namespace SamoBot.Core.Domain;

public sealed record HostPacingUpdate(TimeSpan Delay, double? AvgResponseMs, int ConsecutiveFailures);
