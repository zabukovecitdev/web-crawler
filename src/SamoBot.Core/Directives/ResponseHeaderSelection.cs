using SamoBot.Core.Domain;

namespace SamoBot.Core.Directives;

public sealed class ResponseHeaderSelection(IEnumerable<IResponseHeaderConsumer> consumers)
{
    public IReadOnlyList<string> Names { get; } =
        consumers.SelectMany(consumer => consumer.ResponseHeaders).Distinct(StringComparer.OrdinalIgnoreCase).ToList();

    public ResponseHeaders Collect(Func<string, IEnumerable<string>?> lookup) => ResponseHeaders.From(Names, lookup);

    public async Task<ResponseHeaders> CollectAsync(Func<string, Task<IEnumerable<string>?>> lookup)
    {
        var values = new Dictionary<string, IEnumerable<string>?>(StringComparer.OrdinalIgnoreCase);
        foreach (var name in Names)
        {
            values[name] = await lookup(name);
        }

        return ResponseHeaders.From(Names, name => values[name]);
    }
}
