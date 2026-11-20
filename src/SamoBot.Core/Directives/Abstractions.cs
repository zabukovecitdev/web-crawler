using SamoBot.Core.Domain;

namespace SamoBot.Core.Directives;

public interface IRobotsDirectiveSource
{
    IEnumerable<RobotsDirective> Read(PageContext page);
}

public interface IPagePolicyRule
{
    PagePolicy Apply(PagePolicy policy, PageContext page, IReadOnlyList<RobotsDirective> directives);
}

public interface ILinkFollowRule
{
    bool Allows(ParsedLink link, PageContext page);
}

public interface IRedirectSource
{
    IEnumerable<Uri> Targets(PageContext page);
}

public interface ICanonicalSource
{
    Uri? Find(PageContext page);
}

public interface IResponseHeaderConsumer
{
    IReadOnlyCollection<string> ResponseHeaders { get; }
}
