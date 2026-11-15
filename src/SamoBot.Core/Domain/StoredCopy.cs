using SamoBot.Core.Directives;

namespace SamoBot.Core.Domain;

/// <summary>The newest stored body of a page, with the policy parsed from it. <see cref="PagePolicy.Archive"/> false: do not show it.</summary>
public sealed record StoredCopy(string ObjectName, PagePolicy Policy);
