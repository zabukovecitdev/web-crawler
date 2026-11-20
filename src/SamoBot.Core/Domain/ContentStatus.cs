namespace SamoBot.Core.Domain;

public static class ContentStatus
{
    public const string Stored = "Stored";
    public const string Skipped = "Skipped";
    public const string NotStored = "NotStored";
    public const string Failed = "Failed";

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
