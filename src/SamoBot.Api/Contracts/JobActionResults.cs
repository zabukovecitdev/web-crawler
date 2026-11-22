using FluentResults;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Core.Browser;
using SamoBot.Core.Jobs;

namespace SamoBot.Api.Contracts;

public static class JobActionResults
{
    public static ObjectResult Failed(IEnumerable<IError> errors)
    {
        var list = errors.ToList();
        var detail = string.Join(" ", list.Select(error => error.Message));
        var notFound = list.Any(error => error is JobNotFoundError);
        var status = notFound ? StatusCodes.Status404NotFound : StatusCodes.Status409Conflict;
        var problem = new ProblemDetails { Status = status, Title = notFound ? "Job not found." : "Action not allowed.", Detail = detail };
        if (list.Any(error => error is BrowserUnavailableError))
        {
            problem.Title = "The browser service is not available.";
            problem.Extensions["code"] = BrowserUnavailableError.Code;
        }

        return new ObjectResult(problem) { StatusCode = status };
    }
}
