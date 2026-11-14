using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

/// <summary>
/// Adaptive politeness. The columns are added with a constant default or NULL, so PostgreSQL does not rewrite the table.
/// AvgResponseMs: moving average of the response time of healthy fetches. ConsecutiveFailures: timeouts, network errors,
/// 5xx and 429 in a row. LastDelayMs: the delay applied after the last fetch (the effective pacing, shown on the dashboard).
/// </summary>
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
