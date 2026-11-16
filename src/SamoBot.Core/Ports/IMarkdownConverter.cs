namespace SamoBot.Core.Ports;

/// <summary>Turns stored page HTML into clean Markdown: headings, text, lists, tables, links and images; no scripts, styles or navigation.
/// Relative references are resolved against <paramref name="pageUrl"/>. Pure and thread-safe.</summary>
public interface IMarkdownConverter
{
    string Convert(string html, Uri pageUrl);
}
