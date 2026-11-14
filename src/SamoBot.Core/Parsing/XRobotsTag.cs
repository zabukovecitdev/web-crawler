using System.Text.RegularExpressions;

namespace SamoBot.Core.Parsing;

/// <summary>
/// The X-Robots-Tag response header: the robots meta directives sent as HTTP headers. A response can send the header more
/// than once; the values are kept one per line (<see cref="Join"/>). A value can start with a crawler name
/// ("googlebot: nofollow"), and its directives then apply to that crawler only. Directives without a name apply to all.
/// </summary>
public static partial class XRobotsTag
{
    public const string HeaderName = "X-Robots-Tag";

    /// <summary>Directives that carry a value after a colon, so "name: value" is not a crawler name.</summary>
    private static readonly HashSet<string> DirectivesWithValue = new(StringComparer.OrdinalIgnoreCase)
    {
        "unavailable_after", "max-snippet", "max-image-preview", "max-video-preview"
    };

    /// <summary>The header values, one per line, or null when there are none.</summary>
    public static string? Join(IEnumerable<string>? values)
    {
        var lines = (values ?? []).Select(value => value.Trim()).Where(value => value.Length > 0).ToList();
        return lines.Count == 0 ? null : string.Join('\n', lines);
    }

    /// <summary>True when the header tells <paramref name="userAgent"/> (or every crawler) not to follow the links: nofollow or none.</summary>
    public static bool ForbidsFollow(string? header, string userAgent) =>
        Directives(header, userAgent).Any(directive =>
            directive.Equals("nofollow", StringComparison.OrdinalIgnoreCase) || directive.Equals("none", StringComparison.OrdinalIgnoreCase));

    /// <summary>The directives of the header that apply to <paramref name="userAgent"/>.</summary>
    public static IEnumerable<string> Directives(string? header, string userAgent) =>
        string.IsNullOrWhiteSpace(header)
            ? []
            : header.Split('\n').SelectMany(Line).Where(d => d.Agent is null || d.Agent.Equals(userAgent, StringComparison.OrdinalIgnoreCase)).Select(d => d.Value);

    /// <summary>The directives of one header value. A crawler name applies to the directives after it, up to the next name.</summary>
    private static IEnumerable<(string? Agent, string Value)> Line(string line)
    {
        string? agent = null;
        foreach (var part in line.Split(','))
        {
            var (named, directive) = SplitAgent(part.Trim());
            agent = named ?? agent;
            if (directive.Length > 0)
            {
                yield return (agent, directive);
            }
        }
    }

    /// <summary>"googlebot: nofollow" gives ("googlebot", "nofollow"); "max-snippet: 20" and "nofollow" give no name.</summary>
    private static (string? Agent, string Directive) SplitAgent(string part)
    {
        var match = AgentPrefix().Match(part);
        if (!match.Success || DirectivesWithValue.Contains(match.Groups["agent"].Value))
        {
            return (null, part);
        }

        return (match.Groups["agent"].Value, match.Groups["rest"].Value.Trim());
    }

    [GeneratedRegex(@"^(?<agent>[A-Za-z0-9_\-]+)\s*:(?<rest>.*)$")]
    private static partial Regex AgentPrefix();
}
