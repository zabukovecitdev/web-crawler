using System.Data;
using System.Text.Json;
using Dapper;
using SamoBot.Core.Common;

namespace SamoBot.Adapters.Postgres;

/// <summary>Maps a JSON text column to a list of strings. Dapper keeps the handlers in global state.</summary>
internal static class DapperTypeHandlers
{
    private static readonly Lock Gate = new();
    private static bool _registered;

    public static void Register()
    {
        lock (Gate)
        {
            if (_registered)
            {
                return;
            }

            SqlMapper.AddTypeHandler(new JsonStringListHandler());
            _registered = true;
        }
    }

    private sealed class JsonStringListHandler : SqlMapper.TypeHandler<IReadOnlyList<string>>
    {
        public override void SetValue(IDbDataParameter parameter, IReadOnlyList<string>? value) =>
            parameter.Value = JsonSerializer.Serialize(value ?? [], JsonDefaults.Options);

        public override IReadOnlyList<string> Parse(object value) =>
            JsonSerializer.Deserialize<List<string>>(value.ToString() ?? "[]", JsonDefaults.Options) ?? [];
    }
}
