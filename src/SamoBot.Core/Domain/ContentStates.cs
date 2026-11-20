namespace SamoBot.Core.Domain;

public sealed record PageStateFacts
{
    public int? StatusCode { get; init; }
    public string? ContentType { get; init; }
    public string? ObjectName { get; init; }
    public bool HasParsedDocument { get; init; }
    public bool Indexed { get; init; }
    public string? FailReason { get; init; }
}

public static class ContentStates
{
    public static ContentState Derive(PageStateFacts facts)
    {
        if (facts.StatusCode is not { } code)
        {
            return facts.FailReason is null ? ContentState.NotStored : ContentState.Failed;
        }

        if (code is < 200 or >= 400)
        {
            return ContentState.Failed;
        }

        return code >= 300 ? ContentState.Redirect : SuccessState(facts);
    }

    private static ContentState SuccessState(PageStateFacts facts)
    {
        if (facts.ObjectName is null)
        {
            return IsStoredType(facts.ContentType) ? ContentState.NotStored : ContentState.Skipped;
        }

        if (!facts.HasParsedDocument)
        {
            return ContentState.WaitingForParse;
        }

        return facts.Indexed ? ContentState.ParsedIndexed : ContentState.ParsedWaitingForIndex;
    }

    private static bool IsStoredType(string? contentType) =>
        contentType is null
        || contentType.Contains("html", StringComparison.OrdinalIgnoreCase)
        || StoredMediaTypes.IsStored(StoredMediaTypes.MediaType(contentType));
}
