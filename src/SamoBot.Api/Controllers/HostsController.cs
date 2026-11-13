using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Api.Controllers;

[ApiController]
[Route("api/v1/hosts")]
[Authorize]
public sealed class HostsController(RobotsGate robots, ISiteReader sites, IPageReader pages) : ControllerBase
{
    [HttpGet]
    [ProducesResponseType<SitesPage>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> List(
        [FromQuery] string? query,
        [FromQuery] string? sort,
        [FromQuery] int limit = 50,
        [FromQuery] int offset = 0,
        CancellationToken ct = default)
    {
        var siteQuery = ListQueries.Sites(query, sort, limit, offset);
        return siteQuery is null ? BadRequest("Unknown sort.") : Ok(await sites.ListSites(siteQuery, ct));
    }

    [HttpGet("{host}")]
    [ProducesResponseType<SiteDetail>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> Get(string host, CancellationToken ct)
    {
        var site = await sites.FindSite(host.ToLowerInvariant(), ct);
        return site is null ? NotFound() : Ok(site);
    }

    [HttpGet("{host}/pages")]
    [ProducesResponseType<PagedResult<PageSummary>>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> ListPages(
        string host,
        [FromQuery] string? query,
        [FromQuery] string? state,
        [FromQuery] int limit = 50,
        [FromQuery] int offset = 0,
        CancellationToken ct = default)
    {
        var pageQuery = ListQueries.Pages(query, state, limit, offset);
        return pageQuery is null
            ? BadRequest("Unknown state.")
            : Ok(await pages.ListHostPages(host.ToLowerInvariant(), pageQuery, ct));
    }

    [HttpGet("{host}/robots")]
    [ProducesResponseType<RobotsResponse>(StatusCodes.Status200OK)]
    public async Task<IActionResult> GetRobots(string host, CancellationToken ct) =>
        Ok(RobotsResponse.From(await robots.RulesFor(host, ct)));

    [HttpGet("{host}/sitemaps")]
    [ProducesResponseType<SitemapsResponse>(StatusCodes.Status200OK)]
    public IActionResult GetSitemaps(string host) => Ok(new SitemapsResponse(
        host,
        "Sitemap discovery and persistence are planned; no stored sitemaps yet.",
        []));

    [HttpPost("explain")]
    [ProducesResponseType<ExplainResponse>(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> Explain([FromBody] ExplainRequest request, CancellationToken ct)
    {
        if (!Uri.TryCreate(request.Url, UriKind.Absolute, out var uri))
        {
            return BadRequest("Invalid URL.");
        }

        var rules = await robots.RulesFor(uri.Authority, ct);
        var description = RobotsResponse.From(rules);
        return Ok(new ExplainResponse(
            request.Url,
            uri.Authority,
            RobotsGate.UserAgent,
            rules.IsAllowed(uri.PathAndQuery, RobotsGate.UserAgent),
            description.CrawlDelayMs,
            description));
    }
}
