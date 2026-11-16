using FluentResults;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Core.Jobs;

namespace SamoBot.Api.Contracts;

/// <summary>The answer to a job action that was refused: 404 for an unknown job, else 409 with the reason.</summary>
public static class JobActionResults
{
    public static ObjectResult Failed(IEnumerable<IError> errors)
    {
        var list = errors.ToList();
        var detail = string.Join(" ", list.Select(error => error.Message));
        var notFound = list.Any(error => error is JobNotFoundError);
        var status = notFound ? StatusCodes.Status404NotFound : StatusCodes.Status409Conflict;
        return new ObjectResult(new ProblemDetails { Status = status, Title = notFound ? "Job not found." : "Action not allowed.", Detail = detail })
        {
            StatusCode = status
        };
    }
}
