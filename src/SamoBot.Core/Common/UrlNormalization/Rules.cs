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
        if ((IsScheme(url, Uri.UriSchemeHttp) && url.Port == 80) || (IsScheme(url, Uri.UriSchemeHttps) && url.Port == 443))
        {
            url.Port = -1;
        }
    }

    private static bool IsScheme(UriBuilder url, string scheme) => url.Scheme.Equals(scheme, StringComparison.OrdinalIgnoreCase);
}

public sealed class RemoveTrackingParameters : IQueryRule
{
    private readonly HashSet<string> _names;
    private readonly string _prefix;

    public RemoveTrackingParameters(IEnumerable<string> names, string prefix)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        _names = new HashSet<string>(names, StringComparer.OrdinalIgnoreCase);
        _prefix = prefix;
    }

    public IEnumerable<string> Apply(IEnumerable<string> parameterNames) =>
        parameterNames.Where(name => !_names.Contains(name) && !name.StartsWith(_prefix, StringComparison.OrdinalIgnoreCase));
}

public sealed class SortParameters : IQueryRule
{
    public IEnumerable<string> Apply(IEnumerable<string> parameterNames) =>
        parameterNames.OrderBy(name => name, StringComparer.InvariantCultureIgnoreCase);
}
