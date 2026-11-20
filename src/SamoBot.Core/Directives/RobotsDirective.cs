namespace SamoBot.Core.Directives;

public sealed record RobotsDirective(string Name, string? Value, string Source)
{
    public bool Is(string name) => Name.Equals(name, StringComparison.OrdinalIgnoreCase);

    public override string ToString() => Value is null ? $"{Name} ({Source})" : $"{Name}: {Value} ({Source})";
}
