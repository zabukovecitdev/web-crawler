using System.Text.Json.Serialization;
using SamoBot.Core.Domain;

namespace SamoBot.Core.SiteChecks;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum JsVerdict
{
    NotNeeded,
    Needed,
    ProbablyNeeded
}

public sealed record JavaScriptCheck(JsVerdict Verdict, PageShell Plain, PageShell? Rendered)
{
    public bool Needed => Verdict is JsVerdict.Needed or JsVerdict.ProbablyNeeded;
}
