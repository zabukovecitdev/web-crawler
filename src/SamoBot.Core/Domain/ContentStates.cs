namespace SamoBot.Core.Domain;

/// <summary>What is known about the latest fetch of a URL and its parsed document.</summary>
public sealed record PageStateFacts
{
    public int? StatusCode { get; init; }
    public string? ContentType { get; init; }
    public string? ObjectName { get; init; }
    public bool HasParsedDocument { get; init; }
    public bool Indexed { get; init; }
    public string? FailReason { get; init; }
}

/// <summary>
/// Derives the content state of a page. A URL without a fetch is Failed when it has a fail reason, else NotStored.
/// A status below 200 or from 400 up is Failed. A status in 300-399 is Redirect. Only successful HTML with content is
/// stored: a success without a stored object is Skipped for non-HTML and NotStored for HTML. A stored page waits for
/// the parser until it has a parsed document, then waits for the index until the document is indexed.
/// </summary>
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
            return IsHtml(facts.ContentType) ? ContentState.NotStored : ContentState.Skipped;
        }

        if (!facts.HasParsedDocument)
        {
            return ContentState.WaitingForParse;
        }

        return facts.Indexed ? ContentState.ParsedIndexed : ContentState.ParsedWaitingForIndex;
    }

    private static bool IsHtml(string? contentType) =>
        contentType is null || contentType.Contains("html", StringComparison.OrdinalIgnoreCase);
}
