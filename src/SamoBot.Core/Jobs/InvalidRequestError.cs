using FluentResults;

namespace SamoBot.Core.Jobs;

public sealed class InvalidRequestError(string message) : Error(message);
