using System.Text.RegularExpressions;

namespace SamoBot.Core.Directives;

public static partial class RobotsDirectiveParser
{
    private static readonly HashSet<string> DirectivesWithValue = new(StringComparer.OrdinalIgnoreCase)
    {
        "unavailable_after", "max-snippet", "max-image-preview", "max-video-preview"
    };

    public static IReadOnlyList<RobotsDirective> ParseList(string? content, string source) =>
        Tokens(content).Select(token => Directive(token, source)).ToList();

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
