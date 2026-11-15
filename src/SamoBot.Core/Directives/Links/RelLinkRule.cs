using SamoBot.Core.Domain;

namespace SamoBot.Core.Directives.Links;

/// <summary>
/// A link whose rel attribute contains one of the given values is not followed, such as rel="nofollow". The values are
/// chosen where the rule is registered, so following sponsored or ugc links is a registration change.
/// </summary>
public sealed class RelLinkRule(params string[] blockedRelValues) : ILinkFollowRule
{
    private readonly HashSet<string> _blocked = new(blockedRelValues, StringComparer.OrdinalIgnoreCase);

    public bool Allows(ParsedLink link, PageContext page) =>
        !link.Rel.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries).Any(_blocked.Contains);
}
