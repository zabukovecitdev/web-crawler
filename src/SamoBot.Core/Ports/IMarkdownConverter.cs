namespace SamoBot.Core.Ports;

public interface IMarkdownConverter
{
    string Convert(string html, Uri pageUrl);
}
