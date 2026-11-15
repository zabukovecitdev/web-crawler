namespace SamoBot.Core.Domain;

/// <summary>
/// The response headers of a fetch that the crawler keeps: only the ones a registered directive source asks for
/// (see <see cref="Directives.ResponseHeaderSelection"/>). Names are case-insensitive; a header sent twice has two values.
/// </summary>
public sealed class ResponseHeaders
{
    public static readonly ResponseHeaders None = new([]);

    private readonly Dictionary<string, IReadOnlyList<string>> _values;

    public ResponseHeaders(IEnumerable<KeyValuePair<string, IReadOnlyList<string>>> values)
    {
        _values = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);
        foreach (var (name, list) in values)
        {
            var kept = list.Select(value => value.Trim()).Where(value => value.Length > 0).ToList();
            if (kept.Count > 0)
            {
                _values[name] = kept;
            }
        }
    }

    public bool IsEmpty => _values.Count == 0;

    /// <summary>The values of one header, in the order they were sent. Empty when the header is missing.</summary>
    public IReadOnlyList<string> Values(string name) => _values.TryGetValue(name, out var list) ? list : [];

    public IReadOnlyDictionary<string, IReadOnlyList<string>> ToDictionary() => _values;

    /// <summary>Builds the kept headers from a lookup that returns the values of one header, or null when it is missing.</summary>
    public static ResponseHeaders From(IEnumerable<string> names, Func<string, IEnumerable<string>?> lookup) =>
        new(names.Select(name => new KeyValuePair<string, IReadOnlyList<string>>(name, lookup(name)?.ToList() ?? [])));
}
