namespace SamoBot.Core.Domain;

/// <summary>What a recrawl gave back to the stages: URLs to fetch again, and stored fetches to parse again.</summary>
public sealed record RequeuedWork(int Refetch, int Reparse);
