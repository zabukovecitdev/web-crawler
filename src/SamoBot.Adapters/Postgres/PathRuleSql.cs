namespace SamoBot.Adapters.Postgres;

internal static class PathRuleSql
{
    public static string PathOf(string urlExpression) =>
        $"COALESCE(substring({urlExpression} from '^https?://[^/?#]+(/[^?#]*)'), '/')";

    public static string Allows(string rulesExpression, string pathExpression) => $"""
        COALESCE((
            SELECT r.kind = 'Include'
            FROM jsonb_to_recordset({rulesExpression}) AS r(kind text, prefix text, exact boolean)
            WHERE CASE WHEN COALESCE(r.exact, false)
                       THEN {pathExpression} = r.prefix
                       ELSE starts_with(CASE WHEN right({pathExpression}, 1) = '/' THEN {pathExpression} ELSE {pathExpression} || '/' END, r.prefix)
                  END
            ORDER BY COALESCE(r.exact, false) DESC, length(r.prefix) DESC, (r.kind = 'Include') DESC
            LIMIT 1), true)
        """;

    public const string UseBrowser =
        """("RenderMode" = 'Always' OR ("RenderMode" = 'Auto' AND "JsNeeded"))""";
}
