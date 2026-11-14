using FluentResults;

namespace SamoBot.Core.Jobs;

/// <summary>The job does not exist. The API maps this error to 404.</summary>
public sealed class JobNotFoundError(int id) : Error($"Job {id} was not found.");
