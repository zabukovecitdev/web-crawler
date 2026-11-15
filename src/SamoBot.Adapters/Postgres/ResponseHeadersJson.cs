using System.Text.Json;
using SamoBot.Core.Common;
using SamoBot.Core.Directives;
using SamoBot.Core.Domain;

namespace SamoBot.Adapters.Postgres;

/// <summary>The kept response headers as a JSON object of name to values. No headers is NULL; unreadable JSON is no headers.</summary>
internal static class ResponseHeadersJson
{
    public static string? Write(ResponseHeaders? headers) =>
        headers is null || headers.IsEmpty ? null : JsonSerializer.Serialize(headers.ToDictionary(), JsonDefaults.Options);

    public static ResponseHeaders Read(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return ResponseHeaders.None;
        }

        try
        {
            var values = JsonSerializer.Deserialize<Dictionary<string, List<string>>>(json, JsonDefaults.Options) ?? [];
            return new ResponseHeaders(values.Select(pair => new KeyValuePair<string, IReadOnlyList<string>>(pair.Key, pair.Value)));
        }
        catch (JsonException)
        {
            return ResponseHeaders.None;
        }
    }
}

/// <summary>The stored page policy. A page parsed before policies were stored, or an unreadable value, has the default policy.</summary>
internal static class PagePolicyJson
{
    public static PagePolicy Read(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return PagePolicy.Default;
        }

        try
        {
            return JsonSerializer.Deserialize<PagePolicy>(json, JsonDefaults.Options) ?? PagePolicy.Default;
        }
        catch (JsonException)
        {
            return PagePolicy.Default;
        }
    }
}
