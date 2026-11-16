using System.Globalization;
using System.Text;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Net.Http.Headers;
using SamoBot.Api.Contracts;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1/pages")]
[Authorize]
public sealed class PagesController(IPageReader pages, IContentStore contentStore, IMarkdownConverter markdown) : ControllerBase
{
    public const string NoArchiveMessage = "The site asked crawlers not to show a stored copy of this page (noarchive).";

    [HttpGet("{id:int}")]
    [ProducesResponseType<PageDetail>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Get(int id, CancellationToken ct)
    {
        var page = await pages.FindPage(id, ct);
        return page is null ? NotFound() : Ok(page);
    }

    /// <summary>The links on a page (direction=out, the default) or the pages that link to it (direction=in).</summary>
    [HttpGet("{id:int}/links")]
    [ProducesResponseType<PagedResult<PageLinkItem>>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetLinks(
        int id,
        [FromQuery] string? direction = "out",
        [FromQuery] int limit = 50,
        [FromQuery] int offset = 0,
        CancellationToken ct = default)
    {
        var parsed = LinkListQueries.Parse(direction, limit, offset);
        if (parsed.Query is null)
        {
            return BadRequest(parsed.Error);
        }

        var links = await pages.ListLinks(id, parsed.Query, ct);
        return links is null ? NotFound() : Ok(links);
    }

    /// <summary>The stored body of a page as text. 403 when the site asked for no stored copy to be shown (noarchive).</summary>
    [HttpGet("{id:int}/content")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status403Forbidden)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetContent(int id, CancellationToken ct)
    {
        var (stored, error) = await LoadStoredBody(id, ct);
        if (stored is not { } body)
        {
            return error!;
        }

        SetContentHeaders($"page-{id}.txt", body.Content);
        return File(body.Content.Bytes, "text/plain; charset=utf-8");
    }

    /// <summary>The stored body of a page as clean Markdown, made on demand. 403 when the site asked for no stored copy to be shown (noarchive).
    /// X-Content-Truncated tells that the HTML was cut at the size cap before the conversion.</summary>
    [HttpGet("{id:int}/markdown")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status403Forbidden)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetMarkdown(int id, CancellationToken ct)
    {
        var (stored, error) = await LoadStoredBody(id, ct);
        if (stored is not { } body)
        {
            return error!;
        }

        var result = markdown.Convert(Encoding.UTF8.GetString(body.Content.Bytes), body.Copy.Url);
        SetContentHeaders($"page-{id}.md", body.Content);
        return File(Encoding.UTF8.GetBytes(result), "text/markdown; charset=utf-8");
    }

    /// <summary>Finds the stored copy and loads its capped body. Gives an error result (404, or 403 for noarchive) instead when there is nothing to show.</summary>
    private async Task<(StoredBody? Body, IActionResult? Error)> LoadStoredBody(int id, CancellationToken ct)
    {
        var copy = await pages.FindStoredCopy(id, ct);
        if (copy is null)
        {
            return (null, NotFound());
        }

        if (!copy.Policy.Archive)
        {
            return (null, StatusCode(StatusCodes.Status403Forbidden, new ProblemDetails { Status = StatusCodes.Status403Forbidden, Title = NoArchiveMessage }));
        }

        await using var stream = await contentStore.Load(copy.ObjectName, ct);
        return (new StoredBody(copy, await StoredContent.Read(stream, StoredContent.MaxBytes, ct)), null);
    }

    /// <summary>The body is untrusted page content: forbid sniffing and any active use if a browser opens it directly.</summary>
    private void SetContentHeaders(string fileName, StoredContent content)
    {
        Response.Headers[StoredContent.TruncatedHeader] = content.Truncated ? "true" : "false";
        Response.Headers[StoredContent.StoredBytesHeader] = content.TotalBytes.ToString(CultureInfo.InvariantCulture);
        Response.Headers[HeaderNames.XContentTypeOptions] = "nosniff";
        Response.Headers[HeaderNames.ContentSecurityPolicy] = "default-src 'none'; sandbox";
        Response.Headers[HeaderNames.ContentDisposition] = new ContentDispositionHeaderValue("inline") { FileName = fileName }.ToString();
    }

    private readonly record struct StoredBody(StoredCopy Copy, StoredContent Content);
}
