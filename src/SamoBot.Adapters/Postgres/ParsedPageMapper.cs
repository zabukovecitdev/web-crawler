using System.Text.Json;
using SamoBot.Core.Common;
using SamoBot.Core.Domain;

namespace SamoBot.Adapters.Postgres;

/// <summary>
/// Maps the JSON text columns of a parsed document. Links are not stored, so the link counts are 0.
/// A column that is missing or not valid JSON reads as empty.
/// </summary>
internal static class ParsedPageMapper
{
    public static PageParsed Map(PageRow row) => new()
    {
        Title = row.Title ?? string.Empty,
        Description = row.Description ?? string.Empty,
        Language = row.Language ?? string.Empty,
        Canonical = row.Canonical ?? string.Empty,
        RobotsDirectives = RobotsContent(row.RobotsDirectives),
        BodyText = row.BodyText ?? string.Empty,
        WordCount = WordCount(row.BodyText),
        Headings = Headings(row.Headings),
        ImageCount = ArrayLength(row.Images),
        JsonLdBlocks = ArrayLength(row.JsonLdData),
        OpenGraph = Read<Dictionary<string, string>>(row.OpenGraphData) ?? []
    };

    private static int WordCount(string? text) =>
        string.IsNullOrWhiteSpace(text) ? 0 : text.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries).Length;

    private static IReadOnlyList<PageHeading> Headings(string? json) =>
        (Read<List<ParsedHeading>>(json) ?? []).Select(h => new PageHeading(h.Level, h.Text)).ToList();

    private static string RobotsContent(string? json) => Read<RobotsDirectives>(json)?.Content ?? string.Empty;

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
