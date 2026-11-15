using System.Text.Json;
using SamoBot.Core.Common;
using SamoBot.Core.Domain;

namespace SamoBot.Adapters.Postgres;

/// <summary>
/// Maps the JSON text columns of a parsed document. The link counts come from the Links table (0 for pages parsed before it existed).
/// A column that is missing or not valid JSON reads as empty.
/// </summary>
internal static class ParsedPageMapper
{
    public static PageParsed Map(PageRow row, LinkCounts counts) => new()
    {
        Title = row.Title ?? string.Empty,
        Description = row.Description ?? string.Empty,
        Language = row.Language ?? string.Empty,
        Canonical = row.Canonical ?? string.Empty,
        RobotsDirectives = Directives(row),
        BodyText = row.BodyText ?? string.Empty,
        WordCount = WordCount(row.BodyText),
        Headings = Headings(row.Headings),
        LinkCount = counts.LinkCount,
        InternalLinkCount = counts.InternalLinkCount,
        ImageCount = ArrayLength(row.Images),
        JsonLdBlocks = ArrayLength(row.JsonLdData),
        OpenGraph = Read<Dictionary<string, string>>(row.OpenGraphData) ?? []
    };

    private static int WordCount(string? text) =>
        string.IsNullOrWhiteSpace(text) ? 0 : text.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries).Length;

    private static List<PageHeading> Headings(string? json) =>
        (Read<List<ParsedHeading>>(json) ?? []).Select(h => new PageHeading(h.Level, h.Text)).ToList();

    /// <summary>
    /// The directives that applied, with their source. Pages parsed before policies were stored show the robots meta
    /// content that was stored then.
    /// </summary>
    private static string Directives(PageRow row) =>
        row.Policy is not null
            ? string.Join(", ", PagePolicyJson.Read(row.Policy).Directives)
            : Read<LegacyRobotsDirectives>(row.RobotsDirectives)?.Content ?? string.Empty;

    private sealed record LegacyRobotsDirectives(string Content);

    private static int ArrayLength(string? json) => Read<List<JsonElement>>(json)?.Count ?? 0;

    private static T? Read<T>(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return default;
        }

        try
        {
            return JsonSerializer.Deserialize<T>(json, JsonDefaults.Options);
        }
        catch (JsonException)
        {
            return default;
        }
    }
}
