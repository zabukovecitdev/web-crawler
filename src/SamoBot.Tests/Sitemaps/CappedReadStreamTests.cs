using FluentAssertions;
using SamoBot.Core.Sitemaps;

namespace SamoBot.Tests.Sitemaps;

public class CappedReadStreamTests
{
    private static CappedReadStream Capped(int size, long limit) => new(new MemoryStream(new byte[size]), limit);

    [Fact]
    public void ReadingExactlyTheLimitIsAllowed()
    {
        using var stream = Capped(10, limit: 10);

        stream.Read(new byte[10], 0, 10).Should().Be(10);
        stream.Read(new byte[10], 0, 10).Should().Be(0);
    }

    [Fact]
    public void ReadingPastTheLimitThrows()
    {
        using var stream = Capped(11, limit: 10);

        var read = () => stream.Read(new byte[11], 0, 11);

        read.Should().Throw<InvalidDataException>();
    }

    [Fact]
    public void TheLimitCountsEveryReadTogether()
    {
        using var stream = Capped(12, limit: 10);
        var buffer = new byte[6];

        stream.Read(buffer, 0, 6).Should().Be(6);
        var second = () => stream.Read(buffer, 0, 6);

        second.Should().Throw<InvalidDataException>();
    }

    [Fact]
    public void ReadingIntoASpanCountsToo()
    {
        using var stream = Capped(11, limit: 10);

        var read = () => stream.Read(new byte[11].AsSpan());

        read.Should().Throw<InvalidDataException>();
    }

    [Fact]
    public void ItIsReadOnlyAndForwardOnly()
    {
        using var stream = Capped(1, limit: 10);

        stream.CanRead.Should().BeTrue();
        stream.CanWrite.Should().BeFalse();
        stream.CanSeek.Should().BeFalse();
    }

    [Fact]
    public void DisposingItDisposesTheInnerStream()
    {
        var inner = new MemoryStream(new byte[1]);

        new CappedReadStream(inner, 10).Dispose();

        inner.CanRead.Should().BeFalse();
    }
}
