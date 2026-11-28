using System.Security.Cryptography;
using System.Text;
using FluentAssertions;
using SamoBot.Adapters.Storage;

namespace SamoBot.Tests.Storage;

public class ObjectNameGeneratorTests
{
    private static string Hash(string url) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(url))).ToLowerInvariant();

    [Fact]
    public void GenerateHierarchicalUsesDomainPathAndHash()
    {
        const string url = "https://Example.com/a/b?x=1";

        ObjectNameGenerator.GenerateHierarchical(url, "html").Should().Be($"example.com/a/b/{Hash(url)}.html");
    }

    [Fact]
    public void GenerateHierarchicalForTheRootPathUsesIndex()
    {
        const string url = "https://example.com/";

        ObjectNameGenerator.GenerateHierarchical(url, "html").Should().Be($"example.com/index/{Hash(url)}.html");
    }

    [Fact]
    public void GenerateHierarchicalReplacesUnsafeCharacters()
    {
        const string url = "https://example.com/a b/ü";

        ObjectNameGenerator.GenerateHierarchical(url, "html").Should().StartWith("example.com/a_20b/_C3_BC/");
    }

    [Fact]
    public void GenerateHierarchicalUsesTheGivenExtensionWhateverThePathSays()
    {
        ObjectNameGenerator.GenerateHierarchical("https://example.com/file.PDF", "md").Should().EndWith(".md");
    }

    [Fact]
    public void GenerateHierarchicalForAnInvalidUrlUsesTheHashOnly()
    {
        ObjectNameGenerator.GenerateHierarchical("not a url", "html").Should().Be($"{Hash("not a url")}.html");
    }
}
