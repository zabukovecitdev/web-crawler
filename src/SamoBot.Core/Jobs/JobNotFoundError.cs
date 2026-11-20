using FluentResults;

namespace SamoBot.Core.Jobs;

public sealed class JobNotFoundError(int id) : Error($"Job {id} was not found.");
