using FluentAssertions;
using Microsoft.AspNetCore.Mvc;
using SamoBot.Api.Contracts;
using SamoBot.Api.Controllers;
using SamoBot.Core.Domain;
using SamoBot.Core.Sitemaps;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Api;

public class HostSitemapsControllerTests
{
    private static HostSitemapsController Controller()
    {
        var snapshot = new SitemapSnapshot([], new SitemapCounts(0, 0, 0), new PagedResult<SitemapUrlRow>(0, []));
        return new HostSitemapsController(new SitemapOverviewService(new FakeSitemapReader(snapshot), null!));
    }

    [Theory]
    [InlineData("evil.com/x?")]
    [InlineData("user@host")]
    [InlineData("a b")]
    public async Task ABadHostIsRefused(string host)
    {
        var result = await Controller().GetSitemaps(host);

        result.Should().BeOfType<BadRequestObjectResult>();
    }

    [Fact]
    public async Task AnOffsetBeyondTheCapIsRefused()
    {
        var result = await Controller().GetSitemaps("a.example", offset: ListLimits.MaxOffset + 1);

        result.Should().BeOfType<BadRequestObjectResult>();
    }

    [Fact]
    public async Task AValidHostWithNoSitemapGivesAnEmptyResult()
    {
        var result = await Controller().GetSitemaps("A.Example", offset: ListLimits.MaxOffset);

        result.Should().BeOfType<OkObjectResult>();
    }
}
