namespace SamoBot.Core.Directives;

/// <summary>
/// One robots directive a site sent, such as "noindex" or "max-snippet: 20". <see cref="Name"/> is lower case;
/// <see cref="Value"/> is the text after the colon, or null. <see cref="Source"/> names where it came from, for display.
/// </summary>
public sealed record RobotsDirective(string Name, string? Value, string Source)
{
    public bool Is(string name) => Name.Equals(name, StringComparison.OrdinalIgnoreCase);

    public override string ToString() => Value is null ? $"{Name} ({Source})" : $"{Name}: {Value} ({Source})";
}
