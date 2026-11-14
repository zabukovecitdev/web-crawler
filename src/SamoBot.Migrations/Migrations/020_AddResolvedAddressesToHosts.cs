using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000020, "Cache the resolved IP addresses of a host")]
public class AddResolvedAddressesToHosts : Migration
{
    public override void Up()
    {
        // Comma-separated IP addresses, as the last DNS lookup returned the allowed ones.
        Alter.Table(TableNames.Database.Hosts)
            .AddColumn("ResolvedAddresses").AsString().Nullable()
            .AddColumn("ResolvedAt").AsDateTimeOffset().Nullable();
    }

    public override void Down()
    {
        Delete.Column("ResolvedAddresses").FromTable(TableNames.Database.Hosts);
        Delete.Column("ResolvedAt").FromTable(TableNames.Database.Hosts);
    }
}
