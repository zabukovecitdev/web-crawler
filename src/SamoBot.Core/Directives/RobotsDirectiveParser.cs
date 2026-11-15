using System.Text.RegularExpressions;

namespace SamoBot.Core.Directives;

/// <summary>
/// Reads the comma-separated directive lists of the robots meta tag and the X-Robots-Tag header. Matching is by whole
/// directive, never by substring. A directive with a value is "name: value"; a value that itself contains a comma (an RFC 850
/// date after unavailable_after) is joined back, because no directive name starts with a digit.
/// </summary>
public static partial class RobotsDirectiveParser
{
    /// <summary>Directives that carry a value, so "name: value" is never read as a crawler name.</summary>
    private static readonly HashSet<string> DirectivesWithValue = new(StringComparer.OrdinalIgnoreCase)
    {
        "unavailable_after", "max-snippet", "max-image-preview", "max-video-preview"
    };

    /// <summary>A directive list without crawler names, such as the content of a meta tag.</summary>
    public static IReadOnlyList<RobotsDirective> ParseList(string? content, string source) =>
        Tokens(content).Select(token => Directive(token, source)).ToList();

    /// <summary>
    /// One X-Robots-Tag value. It can start with a crawler name ("googlebot: nofollow"); the name applies to the directives
    /// after it, up to the next name. Only the directives without a name or with <paramref name="userAgent"/> are returned.
    /// </summary>
    public static IReadOnlyList<RobotsDirective> ParseScoped(string? line, string userAgent, string source)
    {
        var result = new List<RobotsDirective>();
        string? agent = null;
        foreach (var token in Tokens(line))
        {
            var (named, rest) = SplitAgent(token);
            agent = named ?? agent;
            if (rest.Length > 0 && (agent is null || agent.Equals(userAgent, StringComparison.OrdinalIgnoreCase)))
            {
                result.Add(Directive(rest, source));
            }
        }

        return result;
    }

    private static List<string> Tokens(string? text)
    {
        var tokens = new List<string>();
        foreach (var part in (text ?? string.Empty).Split(','))
        {
            var token = part.Trim();
            if (token.Length == 0)
            {
                continue;
            }

            if (char.IsAsciiDigit(token[0]) && tokens.Count > 0)
            {
                tokens[^1] = $"{tokens[^1]}, {token}";
                continue;
            }

            tokens.Add(token);
        }

        return tokens;
    }

    private static (string? Agent, string Remainder) SplitAgent(string token)
    {
        var match = NamePrefix().Match(token);
        if (!match.Success || DirectivesWithValue.Contains(match.Groups["name"].Value))
        {
            return (null, token);
        }

        return (match.Groups["name"].Value, match.Groups["rest"].Value.Trim());
    }

    private static RobotsDirective Directive(string token, string source)
    {
        var colon = token.IndexOf(':');
        if (colon < 0)
        {
            return new RobotsDirective(token.ToLowerInvariant(), null, source);
        }

        var value = token[(colon + 1)..].Trim();
        return new RobotsDirective(token[..colon].Trim().ToLowerInvariant(), value.Length == 0 ? null : value, source);
    }

    [GeneratedRegex(@"^(?<name>[A-Za-z0-9_\-]+)\s*:(?<rest>.*)$")]
    private static partial Regex NamePrefix();
}
