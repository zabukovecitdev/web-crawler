using FluentAssertions;
using SamoBot.Migrations.Migrations;

namespace SamoBot.Tests;

public class RepairConcurrentIndexesTests
{
    private static readonly ExpectedIndex HostPages = RepairConcurrentIndexes.Indexes[1];

    [Theory]
    [InlineData(true, """CREATE INDEX "x" ON public."DiscoveredUrls" USING btree ("Host", "LastCrawlAt" DESC NULLS LAST, "Id")""", true)]
    [InlineData(false, """CREATE INDEX "x" ON public."DiscoveredUrls" USING btree ("Host", "LastCrawlAt" DESC NULLS LAST, "Id")""", false)]
    [InlineData(true, """CREATE INDEX "x" ON public."DiscoveredUrls" USING btree ("Host", "LastCrawlAt" DESC)""", false)]
    public void AnIndexIsHealthyWhenItIsValidAndHasTheExpectedColumns(bool valid, string definition, bool healthy) =>
        RepairConcurrentIndexes.IsHealthy(valid, definition, HostPages).Should().Be(healthy);
}
