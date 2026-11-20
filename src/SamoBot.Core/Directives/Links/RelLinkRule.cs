using SamoBot.Core.Domain;

namespace SamoBot.Core.Directives.Links;

public sealed class RelLinkRule(params string[] blockedRelValues) : ILinkFollowRule
{
    private readonly HashSet<string> _blocked = new(blockedRelValues, StringComparer.OrdinalIgnoreCase);

    public bool Allows(ParsedLink link, PageContext page) =>
        !link.Rel.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries).Any(_blocked.Contains);
}
