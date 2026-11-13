namespace SamoBot.Core.Domain;

/// <summary>One page of a list and the total number of rows behind it.</summary>
public sealed record PagedResult<T>(int Total, IReadOnlyList<T> Items);
