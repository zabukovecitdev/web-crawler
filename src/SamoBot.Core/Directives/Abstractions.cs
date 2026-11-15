using SamoBot.Core.Domain;

namespace SamoBot.Core.Directives;

/// <summary>
/// A place a site can send robots directives: a meta tag, a header. Each source returns only the directives that apply to
/// this crawler. Add a source to read directives from somewhere new; the rules do not change.
/// </summary>
public interface IRobotsDirectiveSource
{
    IEnumerable<RobotsDirective> Read(PageContext page);
}

/// <summary>
/// One decision about a page. A rule receives the policy so far and returns it, possibly stricter. Add a rule to support a
/// new directive or signal; the other rules and the evaluator do not change.
/// </summary>
public interface IPagePolicyRule
{
    PagePolicy Apply(PagePolicy policy, PageContext page, IReadOnlyList<RobotsDirective> directives);
}

/// <summary>Decides whether one link of a page may be followed. A link is followed only when every rule allows it.</summary>
public interface ILinkFollowRule
{
    bool Allows(ParsedLink link, PageContext page);
}

/// <summary>
/// URLs a page points to as a replacement for itself (a meta refresh, a canonical). They are queued like an HTTP redirect:
/// at the depth of the page, and whether or not its links may be followed.
/// </summary>
public interface IRedirectSource
{
    IEnumerable<Uri> Targets(PageContext page);
}

/// <summary>A place a page can name its canonical URL. The first source that finds one wins, in registration order.</summary>
public interface ICanonicalSource
{
    Uri? Find(PageContext page);
}

/// <summary>A source that reads response headers names them here, so the fetchers keep those headers with the fetch.</summary>
public interface IResponseHeaderConsumer
{
    IReadOnlyCollection<string> ResponseHeaders { get; }
}
