using SamoBot.Core.Domain;

namespace SamoBot.Core.Browser;

public static class JsDetector
{
    public const int ThinText = 300;
    public const int ScriptHeavyCount = 3;
    public const int ScriptHeavyBytes = 10_000;
    public const int FewLinks = 2;
    public const int FewLinksText = 500;
    public const int NoticeText = 500;
    public const int MinTextGain = 500;
    public const int MinLinkGain = 10;
    public const int GainFactor = 2;

    public static bool IsSuspect(PageShell page) =>
        page.EmptyAppContainer || NoticeOnThinPage(page) || ThinPageWithHeavyScripts(page) || FewLinksOnThinPage(page);

    private static bool NoticeOnThinPage(PageShell page) => page.NoscriptNotice && page.TextLength < NoticeText;

    private static bool ThinPageWithHeavyScripts(PageShell page) =>
        page.TextLength < ThinText && (page.ScriptCount >= ScriptHeavyCount || page.InlineScriptBytes >= ScriptHeavyBytes);

    private static bool FewLinksOnThinPage(PageShell page) =>
        page.LinkCount <= FewLinks && page.TextLength < FewLinksText && page.ScriptCount >= 1;

    public static bool NeedsJs(PageShell plain, PageShell rendered) =>
        Gained(plain.TextLength, rendered.TextLength, MinTextGain) || Gained(plain.LinkCount, rendered.LinkCount, MinLinkGain);

    private static bool Gained(int plain, int rendered, int minimum) =>
        rendered - plain >= minimum && rendered >= plain * GainFactor;
}
