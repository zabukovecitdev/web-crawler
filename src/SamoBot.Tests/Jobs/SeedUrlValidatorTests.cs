using FluentAssertions;
using SamoBot.Core.Jobs;

namespace SamoBot.Tests.Jobs;

public class SeedUrlValidatorTests
{
    private static IReadOnlyList<string> Validate(params string?[] seeds) => SeedUrlValidator.Validate(seeds);

    [Theory]
    [InlineData("https://example.com/")]
    [InlineData("http://example.com")]
    [InlineData("HTTPS://Sub.Example.COM:8443/path?q=1#frag")]
    [InlineData("https://example.com./")]
    [InlineData("https://8.8.8.8/")]
    [InlineData("https://[2606:4700:4700::1111]/")]
    [InlineData("https://172.32.0.1/")]
    [InlineData("https://100.63.255.255/")]
    [InlineData("https://192.169.0.1/")]
    [InlineData("https://xn--bcher-kva.example/")]
    public void AcceptsPublicHttpUrls(string seed) => Validate(seed).Should().BeEmpty();

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("not a url")]
    [InlineData("/relative/path")]
    [InlineData("example.com")]
    public void RejectsEmptyAndNonAbsoluteUrls(string seed) => Validate(seed).Should().ContainSingle();

    [Theory]
    [InlineData("ftp://example.com/")]
    [InlineData("file:///etc/passwd")]
    [InlineData("javascript:alert(1)")]
    [InlineData("gopher://example.com/")]
    [InlineData("data:text/html,hi")]
    [InlineData("mailto:a@example.com")]
    public void RejectsOtherSchemes(string seed) => Validate(seed).Should().ContainSingle().Which.Should().Contain("http");

    [Theory]
    [InlineData("https://user@example.com/")]
    [InlineData("https://user:secret@example.com/")]
    [InlineData("https://:secret@example.com/")]
    public void RejectsCredentialsInTheUrl(string seed) => Validate(seed).Should().ContainSingle().Which.Should().Contain("password");

    [Theory]
    [InlineData("http://127.0.0.1/")]
    [InlineData("http://127.1.2.3:8080/")]
    [InlineData("http://0.0.0.0/")]
    [InlineData("http://10.0.0.1/")]
    [InlineData("http://10.255.255.255/")]
    [InlineData("http://172.16.0.1/")]
    [InlineData("http://172.31.255.255/")]
    [InlineData("http://192.168.1.1/")]
    [InlineData("http://169.254.169.254/latest/meta-data/")]
    [InlineData("http://169.254.0.1/")]
    [InlineData("http://100.64.0.1/")]
    [InlineData("http://100.127.255.255/")]
    [InlineData("http://192.0.0.8/")]
    [InlineData("http://198.18.0.1/")]
    [InlineData("http://224.0.0.1/")]
    [InlineData("http://255.255.255.255/")]
    [InlineData("http://2130706433/")]
    [InlineData("http://0x7f.0.0.1/")]
    [InlineData("http://[::1]/")]
    [InlineData("http://[::]/")]
    [InlineData("http://[fe80::1]/")]
    [InlineData("http://[fc00::1]/")]
    [InlineData("http://[fd00:ec2::254]/")]
    [InlineData("http://[ff02::1]/")]
    [InlineData("http://[::ffff:127.0.0.1]/")]
    [InlineData("http://[::ffff:10.0.0.1]/")]
    [InlineData("http://[::ffff:169.254.169.254]/")]
    [InlineData("http://[64:ff9b::7f00:1]/")]
    public void RejectsLoopbackPrivateLinkLocalAndMetadataAddresses(string seed) =>
        Validate(seed).Should().ContainSingle().Which.Should().Contain("address");

    [Theory]
    [InlineData("http://localhost/")]
    [InlineData("http://LOCALHOST:3000/")]
    [InlineData("http://localhost./")]
    [InlineData("http://app.localhost/")]
    [InlineData("http://printer.local/")]
    [InlineData("http://db.internal/")]
    [InlineData("http://metadata.google.internal/computeMetadata/v1/")]
    [InlineData("http://router.home.arpa/")]
    [InlineData("http://intranet/")]
    public void RejectsLocalHostNames(string seed) =>
        Validate(seed).Should().ContainSingle().Which.Should().Contain("host name");

    [Fact]
    public void RejectsAnEmptyList()
    {
        SeedUrlValidator.Validate([]).Should().ContainSingle().Which.Should().Contain("At least one");
        SeedUrlValidator.Validate(null).Should().ContainSingle();
    }

    [Fact]
    public void AllowsTenSeedsAndRejectsEleven()
    {
        var seeds = Enumerable.Range(1, 10).Select(i => $"https://site{i}.example/").ToList();

        SeedUrlValidator.Validate(seeds).Should().BeEmpty();
        SeedUrlValidator.Validate([.. seeds, "https://site11.example/"]).Should().ContainSingle().Which.Should().Contain("At most 10");
    }

    [Fact]
    public void LimitsTheLengthOfASeed()
    {
        var atLimit = "https://example.com/" + new string('a', SeedUrlValidator.MaxLength - 20);

        Validate(atLimit).Should().BeEmpty();
        Validate(atLimit + "a").Should().ContainSingle().Which.Should().Contain("2048");
    }

    [Fact]
    public void NamesTheSeedThatFailsAndReportsEveryProblem()
    {
        var errors = Validate("https://ok.example/", "http://localhost/", "ftp://x.example/");

        errors.Should().HaveCount(2);
        errors[0].Should().StartWith("Seed URL 2:");
        errors[1].Should().StartWith("Seed URL 3:");
    }
}
