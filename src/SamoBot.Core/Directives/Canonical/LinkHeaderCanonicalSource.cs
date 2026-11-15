using System.Text.RegularExpressions;

namespace SamoBot.Core.Directives.Canonical;

/// <summary>
/// The HTTP Link header with rel="canonical" (RFC 8288), the way PDFs and other non-HTML files name a canonical URL.
/// A relative target resolves against the page URL, not the &lt;base&gt; of the HTML.
/// </summary>
public sealed partial class LinkHeaderCanonicalSource : ICanonicalSource, IResponseHeaderConsumer
{
    public const string HeaderName = "Link";

    public IReadOnlyCollection<string> ResponseHeaders { get; } = [HeaderName];

    public Uri? Find(PageContext page) =>
        page.Headers.Values(HeaderName)
            .SelectMany(value => LinkValue().Matches(value))
            .Where(match => IsCanonical(match.Groups["params"].Value))
            .Select(match => PageContext.Resolve(page.PageUrl, match.Groups["url"].Value))
            .FirstOrDefault(uri => uri is not null);

    private static bool IsCanonical(string parameters) =>
        RelParameter().Matches(parameters)
            .SelectMany(match => match.Groups["rel"].Value.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            .Any(rel => rel.Equals("canonical", StringComparison.OrdinalIgnoreCase));

    [GeneratedRegex(@"<(?<url>[^>]*)>(?<params>[^,<]*)")]
    private static partial Regex LinkValue();

    [GeneratedRegex(@";\s*rel\s*=\s*(?:""(?<rel>[^""]*)""|(?<rel>[^;\s]+))", RegexOptions.IgnoreCase)]
    private static partial Regex RelParameter();
}
