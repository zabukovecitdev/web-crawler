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

        SetContentHeaders($"page-{id}.txt", body);
        return File(body.Content.Bytes, "text/plain; charset=utf-8");
    }

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

        var text = Encoding.UTF8.GetString(body.Content.Bytes);
        var result = IsHtml(body.Copy.ContentType, text) ? markdown.Convert(text, body.Copy.Url) : text;
        SetContentHeaders($"page-{id}.md", body);
        return File(Encoding.UTF8.GetBytes(result), "text/markdown; charset=utf-8");
    }

    private static bool IsHtml(string? contentType, string text)
    {
        var mediaType = StoredMediaTypes.MediaType(contentType);
        if (StoredMediaTypes.IsMarkdown(mediaType))
        {
            return false;
        }

        return !StoredMediaTypes.IsPlainText(mediaType) || StoredMediaTypes.LooksLikeHtml(text);
    }

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

    private void SetContentHeaders(string fileName, StoredBody body)
    {
        Response.Headers[StoredContent.TruncatedHeader] = body.Content.Truncated ? "true" : "false";
        Response.Headers[StoredContent.StoredBytesHeader] = body.Content.TotalBytes.ToString(CultureInfo.InvariantCulture);
        Response.Headers[StoredContent.CutAtCrawlHeader] = body.Copy.CutAtCrawl ? "true" : "false";
        Response.Headers[HeaderNames.XContentTypeOptions] = "nosniff";
        Response.Headers[HeaderNames.ContentSecurityPolicy] = "default-src 'none'; sandbox";
        Response.Headers[HeaderNames.ContentDisposition] = new ContentDispositionHeaderValue("inline") { FileName = fileName }.ToString();
    }

    private readonly record struct StoredBody(StoredCopy Copy, StoredContent Content);
}
