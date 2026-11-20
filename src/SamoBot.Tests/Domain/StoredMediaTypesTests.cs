using FluentAssertions;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Domain;

public class StoredMediaTypesTests
{
    [Theory]
    [InlineData("text/html; charset=utf-8", "html")]
    [InlineData("application/xhtml+xml", "html")]
    [InlineData("text/markdown; charset=utf-8", "md")]
    [InlineData("TEXT/X-MARKDOWN", "md")]
    [InlineData("text/plain", "txt")]
    [InlineData("application/octet-stream", "html")]
    [InlineData(null, "html")]
    public void FileExtensionFollowsTheStoredFormat(string? contentType, string extension) =>
        StoredMediaTypes.FileExtension(contentType).Should().Be(extension);
}
