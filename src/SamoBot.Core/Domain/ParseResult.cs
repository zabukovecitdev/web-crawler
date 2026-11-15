using SamoBot.Core.Directives;

namespace SamoBot.Core.Domain;

/// <summary>Everything one parse produces: the page, its policy, the URLs it adds to the frontier, and its stored links.</summary>
public sealed record ParseResult(ParsedPage Page, PagePolicy Policy, IReadOnlyList<NewUrl> NewUrls, IReadOnlyList<PageLink> PageLinks);
