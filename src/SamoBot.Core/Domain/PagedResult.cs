namespace SamoBot.Core.Domain;

public sealed record PagedResult<T>(int Total, IReadOnlyList<T> Items);
