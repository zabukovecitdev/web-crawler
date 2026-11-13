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

        ObjectNameGenerator.GenerateHierarchical(url, "text/html").Should().Be($"example.com/a/b/{Hash(url)}.html");
    }

    [Fact]
    public void GenerateHierarchicalForTheRootPathUsesIndex()
    {
        const string url = "https://example.com/";

        ObjectNameGenerator.GenerateHierarchical(url, "text/html").Should().Be($"example.com/index/{Hash(url)}.html");
    }

    [Fact]
    public void GenerateHierarchicalReplacesUnsafeCharacters()
    {
        const string url = "https://example.com/a b/ü";

        ObjectNameGenerator.GenerateHierarchical(url).Should().StartWith("example.com/a_20b/_C3_BC/");
    }

    [Theory]
    [InlineData("application/pdf", "https://example.com/x", "pdf")]
    [InlineData("text/plain; charset=utf-8", "https://example.com/x", "txt")]
    [InlineData(null, "https://example.com/file.PDF", "pdf")]
    [InlineData("application/unknown", "https://example.com/x", "html")]
    public void GenerateHierarchicalChoosesTheExtensionFromTheMediaTypeThenThePath(string? contentType, string url, string extension)
    {
        ObjectNameGenerator.GenerateHierarchical(url, contentType).Should().EndWith($".{extension}");
    }

    [Fact]
    public void GenerateHierarchicalForAnInvalidUrlUsesTheHashOnly()
    {
        ObjectNameGenerator.GenerateHierarchical("not a url", "text/html").Should().Be($"{Hash("not a url")}.html");
    }
}
