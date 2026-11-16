using SamoBot.Core.Directives;

namespace SamoBot.Core.Domain;

/// <summary>The newest stored body of a page, the address it was fetched from (the final URL after redirects, else the page URL), and the policy parsed from it.
/// <see cref="PagePolicy.Archive"/> false: do not show it.</summary>
public sealed record StoredCopy(string ObjectName, Uri Url, PagePolicy Policy);
