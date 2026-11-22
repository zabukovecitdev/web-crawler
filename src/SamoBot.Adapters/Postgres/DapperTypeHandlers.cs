using System.Data;
using System.Text.Json;
using Dapper;
using SamoBot.Core.Common;
using SamoBot.Core.Directives;
using SamoBot.Core.Domain;

namespace SamoBot.Adapters.Postgres;

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
            SqlMapper.AddTypeHandler(new ResponseHeadersHandler());
            SqlMapper.AddTypeHandler(new PagePolicyHandler());
            SqlMapper.AddTypeHandler(new PathRuleListHandler());
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

    private sealed class PathRuleListHandler : SqlMapper.TypeHandler<IReadOnlyList<PathRule>>
    {
        public override void SetValue(IDbDataParameter parameter, IReadOnlyList<PathRule>? value) =>
            parameter.Value = JsonSerializer.Serialize(value ?? [], JsonDefaults.Options);

        public override IReadOnlyList<PathRule> Parse(object value) =>
            JsonSerializer.Deserialize<List<PathRule>>(value.ToString() ?? "[]", JsonDefaults.Options) ?? [];
    }

    private sealed class ResponseHeadersHandler : SqlMapper.TypeHandler<ResponseHeaders>
    {
        public override void SetValue(IDbDataParameter parameter, ResponseHeaders? value) =>
            parameter.Value = (object?)ResponseHeadersJson.Write(value) ?? DBNull.Value;

        public override ResponseHeaders Parse(object value) => ResponseHeadersJson.Read(value.ToString());
    }

    private sealed class PagePolicyHandler : SqlMapper.TypeHandler<PagePolicy>
    {
        public override void SetValue(IDbDataParameter parameter, PagePolicy? value) =>
            parameter.Value = JsonSerializer.Serialize(value ?? PagePolicy.Default, JsonDefaults.Options);

        public override PagePolicy Parse(object value) => PagePolicyJson.Read(value.ToString());
    }
}
