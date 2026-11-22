using FluentResults;

namespace SamoBot.Core.Browser;

public sealed class BrowserUnavailableError : Error
{
    public const string Code = "browser-unavailable";

    public BrowserUnavailableError(string message)
        : base(message) => Metadata["code"] = Code;
}
