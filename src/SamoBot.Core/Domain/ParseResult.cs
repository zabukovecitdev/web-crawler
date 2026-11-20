using SamoBot.Core.Directives;

namespace SamoBot.Core.Domain;

public sealed record ParseResult(ParsedPage Page, PagePolicy Policy, IReadOnlyList<NewUrl> NewUrls, IReadOnlyList<PageLink> PageLinks);
