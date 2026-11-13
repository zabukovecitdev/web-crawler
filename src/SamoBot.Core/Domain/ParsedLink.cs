namespace SamoBot.Core.Domain;

public sealed record ParsedLink
{
    public string Url { get; init; } = string.Empty;
    public string LinkText { get; init; } = string.Empty;
    public string Rel { get; init; } = string.Empty;
    public string Title { get; init; } = string.Empty;
    public bool IsNoFollow { get; init; }
}
