using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000024, "Add the adaptive pacing state to Hosts")]
public class AddPacingToHosts : Migration
{
    private const string Table = TableNames.Database.Hosts;

    public override void Up() =>
        Alter.Table(Table)
            .AddColumn("AvgResponseMs").AsDouble().Nullable()
            .AddColumn("ConsecutiveFailures").AsInt32().NotNullable().WithDefaultValue(0)
            .AddColumn("LastDelayMs").AsInt32().Nullable();

    public override void Down()
    {
        Delete.Column("AvgResponseMs").FromTable(Table);
        Delete.Column("ConsecutiveFailures").FromTable(Table);
        Delete.Column("LastDelayMs").FromTable(Table);
    }
}
