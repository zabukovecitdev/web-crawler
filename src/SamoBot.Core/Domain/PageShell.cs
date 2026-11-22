namespace SamoBot.Core.Domain;

public sealed record PageShell(
    int TextLength,
    int LinkCount,
    int ScriptCount,
    int InlineScriptBytes,
    bool EmptyAppContainer,
    bool NoscriptNotice)
{
    public static readonly PageShell Empty = new(0, 0, 0, 0, false, false);
}
