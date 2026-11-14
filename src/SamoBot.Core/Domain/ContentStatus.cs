namespace SamoBot.Core.Domain;

/// <summary>What happened to the body of a fetch. The dashboard reads these names.</summary>
public static class ContentStatus
{
    public const string Stored = "Stored";
    public const string Skipped = "Skipped";
    public const string NotStored = "NotStored";
    public const string Failed = "Failed";

    /// <summary>
    /// Skipped also covers a blocked address (the fetch never ran). Failed: no response (status 0). Stored: the body is in the content store.
    /// Skipped: a success with a body that the crawler does not keep (not HTML, or empty).
    /// NotStored: every other response (redirects and error statuses).
    /// </summary>
    public static string Of(FetchResponse response, string? objectName)
    {
        if (response.IsBlocked)
        {
            return Skipped;
        }

        if (response.StatusCode == 0)
        {
            return Failed;
        }

        if (objectName is not null)
        {
            return Stored;
        }

        return response.IsSuccess ? Skipped : NotStored;
    }
}
