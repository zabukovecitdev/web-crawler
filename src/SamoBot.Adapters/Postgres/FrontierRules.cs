namespace SamoBot.Adapters.Postgres;

/// <summary>
/// The one definition of the frontier states that several queries share.
/// </summary>
internal static class FrontierRules
{
    /// <summary>SQL that is true when the DiscoveredUrls row aliased <paramref name="alias"/> waits for a fetch: new, requeued or retrying.</summary>
    public static string WaitingForFetch(string alias) =>
        $"""({alias}."Status" = 'Idle' AND ({alias}."LastCrawlAt" IS NULL OR {alias}."FailCount" > 0))""";
}
