namespace SamoBot.Core.Domain;

public sealed record NewUrl(int CrawlJobId, string Url, string NormalizedUrl, string Host, int Depth);
