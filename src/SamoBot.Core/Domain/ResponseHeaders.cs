namespace SamoBot.Core.Domain;

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

    public IReadOnlyList<string> Values(string name) => _values.TryGetValue(name, out var list) ? list : [];

    public IReadOnlyDictionary<string, IReadOnlyList<string>> ToDictionary() => _values;

    public static ResponseHeaders From(IEnumerable<string> names, Func<string, IEnumerable<string>?> lookup) =>
        new(names.Select(name => new KeyValuePair<string, IReadOnlyList<string>>(name, lookup(name)?.ToList() ?? [])));
}
