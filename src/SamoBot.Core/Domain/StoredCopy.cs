using SamoBot.Core.Directives;

namespace SamoBot.Core.Domain;

public sealed record StoredCopy(string ObjectName, Uri Url, PagePolicy Policy, string? ContentType);
