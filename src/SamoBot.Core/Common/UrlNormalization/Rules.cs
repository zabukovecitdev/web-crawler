namespace SamoBot.Core.Common;

public interface IUrlRule
{
    void Apply(UriBuilder url);
}

public interface IQueryRule
{
    IEnumerable<string> Apply(IEnumerable<string> parameterNames);
}

public sealed class LowerCaseSchemeAndHost : IUrlRule
{
    public void Apply(UriBuilder url)
    {
        url.Scheme = url.Scheme.ToLowerInvariant();
        url.Host = url.Host.ToLowerInvariant();
    }
}

public sealed class DropFragment : IUrlRule
{
    public void Apply(UriBuilder url) => url.Fragment = string.Empty;
}

public sealed class DropDefaultPort : IUrlRule
{
    public void Apply(UriBuilder url)
    {
        if ((url.Scheme == Uri.UriSchemeHttp && url.Port == 80) || (url.Scheme == Uri.UriSchemeHttps && url.Port == 443))
        {
            url.Port = -1;
        }
    }
}

public sealed class RemoveTrackingParameters(IReadOnlySet<string> names, string prefix) : IQueryRule
{
    public IEnumerable<string> Apply(IEnumerable<string> parameterNames) =>
        parameterNames.Where(name => !names.Contains(name) && !name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase));
}

public sealed class SortParameters : IQueryRule
{
    public IEnumerable<string> Apply(IEnumerable<string> parameterNames) =>
        parameterNames.OrderBy(name => name, StringComparer.InvariantCultureIgnoreCase);
}
