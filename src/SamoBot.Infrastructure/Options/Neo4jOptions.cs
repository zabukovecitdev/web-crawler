namespace SamoBot.Infrastructure.Options;

public class Neo4jOptions
{
    public const string SectionName = "Neo4j";
    public string Uri { get; set; } = "bolt://localhost:7687";
    public string Username { get; set; } = "neo4j";
    public string Password { get; set; } = "password";
    public bool Enabled { get; set; } = true;
}
