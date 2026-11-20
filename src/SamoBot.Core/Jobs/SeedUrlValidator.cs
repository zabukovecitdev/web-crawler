namespace SamoBot.Core.Jobs;

public static class SeedUrlValidator
{
    public const int MaxSeeds = 10;
    public const int MaxLength = 2048;

    public static IReadOnlyList<string> Validate(IReadOnlyList<string?>? seeds)
    {
        if (seeds is null || seeds.Count == 0)
        {
            return ["At least one seed URL is required."];
        }

        var errors = new List<string>();
        if (seeds.Count > MaxSeeds)
        {
            errors.Add($"At most {MaxSeeds} seed URLs are allowed.");
        }

        for (var index = 0; index < seeds.Count; index++)
        {
            var problem = Check(seeds[index]);
            if (problem is not null)
            {
                errors.Add($"Seed URL {index + 1}: {problem}");
            }
        }

        return errors;
    }

    private static string? Check(string? seed)
    {
        if (string.IsNullOrWhiteSpace(seed))
        {
            return "the URL is empty.";
        }

        if (seed.Length > MaxLength)
        {
            return $"the URL is longer than {MaxLength} characters.";
        }

        if (!Uri.TryCreate(seed.Trim(), UriKind.Absolute, out var uri))
        {
            return "the URL is not an absolute URL.";
        }

        return CheckParts(uri);
    }

    private static string? CheckParts(Uri uri)
    {
        if (uri.Scheme is not ("http" or "https"))
        {
            return "only http and https URLs are allowed.";
        }

        if (uri.UserInfo.Length > 0)
        {
            return "the URL must not contain a user name or password.";
        }

        return HostRules.Check(uri.DnsSafeHost);
    }
}
