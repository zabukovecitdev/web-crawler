namespace SamoBot.Core.Common;

public static class TrackingParameters
{
    public const string UtmPrefix = "utm_";

    public static readonly IReadOnlySet<string> Names = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "utm_id",
        "fbclid", "gclid", "igshid", "twclid", "li_fat_id", "_ga", "_gl", "yclid",
        "mc_cid", "mc_eid", "mkt_tok", "_hsenc", "_hsmi", "hsCtaTracking",
        "trk", "trk_info", "ncid", "clickid", "click_id",
        "affiliate", "affiliateId", "affiliate_id", "partner", "partnerId", "partner_id"
    };
}
