namespace SamoBot.Core.Domain;

public sealed record ParsedPage
{
    public string Title { get; init; } = string.Empty;
    public string Description { get; init; } = string.Empty;
    public string Keywords { get; init; } = string.Empty;
    public string Author { get; init; } = string.Empty;
    public string Language { get; init; } = string.Empty;
    public string Canonical { get; init; } = string.Empty;
    public string BodyText { get; init; } = string.Empty;
    public IReadOnlyList<ParsedHeading> Headings { get; init; } = [];
    public IReadOnlyList<ParsedLink> Links { get; init; } = [];
    public IReadOnlyList<ParsedImage> Images { get; init; } = [];
    public string BaseHref { get; init; } = string.Empty;
    public IReadOnlyList<MetaTag> MetaTags { get; init; } = [];
    public IReadOnlyDictionary<string, string> OpenGraphData { get; init; } = new Dictionary<string, string>();
    public IReadOnlyDictionary<string, string> TwitterCardData { get; init; } = new Dictionary<string, string>();
    public IReadOnlyList<string> JsonLdData { get; init; } = [];
    public PageShell Shell { get; init; } = PageShell.Empty;
}

public sealed record ParsedHeading(int Level, string Text);

public sealed record ParsedImage
{
    public string Src { get; init; } = string.Empty;
    public string Alt { get; init; } = string.Empty;
    public string Title { get; init; } = string.Empty;
    public string Width { get; init; } = string.Empty;
    public string Height { get; init; } = string.Empty;
}
