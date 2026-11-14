using FluentAssertions;
using SamoBot.Api.Contracts;

namespace SamoBot.Tests.Api;

public class HostKeyTests
{
    [Theory]
    [InlineData("example.com", "example.com")]
    [InlineData("Example.COM:8080", "example.com:8080")]
    [InlineData("[::1]:8080", "[::1]:8080")]
    [InlineData("127.0.0.1", "127.0.0.1")]
    public void AcceptsAndLowercasesAnAuthority(string value, string expected)
    {
        HostKey.TryNormalize(value, out var host).Should().BeTrue();
        host.Should().Be(expected);
    }

    [Theory]
    [InlineData("evil.com/x?")]
    [InlineData("user@host")]
    [InlineData("host#x")]
    [InlineData("a b")]
    [InlineData("")]
    [InlineData(null)]
    public void RejectsAnythingThatIsNotAPlainAuthority(string? value) =>
        HostKey.TryNormalize(value, out _).Should().BeFalse();

    [Fact]
    public void RejectsAValueOver260Characters() =>
        HostKey.TryNormalize(new string('a', 300) + ".com", out _).Should().BeFalse();
}
