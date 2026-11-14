using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

/// <summary>
/// The X-Robots-Tag response header of a fetch, one value per line. The parser reads it with the fetch, so a nofollow sent
/// as a header stops the links of the page from being followed. NULL when the response had no such header.
/// </summary>
[Migration(20240101000026, "Add the X-Robots-Tag header to UrlFetches")]
public class AddXRobotsTagToUrlFetches : Migration
{
    private const string Table = TableNames.Database.UrlFetches;

    public override void Up() =>
        Alter.Table(Table).AddColumn("XRobotsTag").AsString().Nullable();

    public override void Down() =>
        Delete.Column("XRobotsTag").FromTable(Table);
}
