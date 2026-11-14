using System.Text.Json;
using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Domain;

public class CrawlEventEnvelopeTests
{
    private static JsonElement Build(string? payload, long? id = null) =>
        JsonSerializer.SerializeToElement(CrawlEventEnvelope.Build(7, "FetchStarted", TestData.Now, payload, id));

    [Fact]
    public void FlattensThePayloadNextToTheEnvelopeFields()
    {
        var json = Build("""{"url":"https://a.example/","depth":1}""");

        json.GetProperty("crawlJobId").GetInt32().Should().Be(7);
        json.GetProperty("eventType").GetString().Should().Be("FetchStarted");
        json.GetProperty("at").GetString().Should().Be("2026-01-01T12:00:00.0000000Z");
        json.GetProperty("url").GetString().Should().Be("https://a.example/");
        json.GetProperty("depth").GetInt32().Should().Be(1);
        json.TryGetProperty("payload", out _).Should().BeFalse();
        json.TryGetProperty("id", out _).Should().BeFalse();
    }

    [Fact]
    public void AddsTheIdWhenTheEventIsStored() => Build("{}", 12).GetProperty("id").GetInt64().Should().Be(12);

    [Fact]
    public void ReservedNamesInThePayloadDoNotWin()
    {
        var json = Build("""{"eventType":"Fake","crawlJobId":99,"at":"x","id":5}""", 1);

        json.GetProperty("eventType").GetString().Should().Be("FetchStarted");
        json.GetProperty("crawlJobId").GetInt32().Should().Be(7);
        json.GetProperty("at").GetString().Should().StartWith("2026");
        json.GetProperty("id").GetInt64().Should().Be(1);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("not json")]
    [InlineData("[1,2]")]
    public void ABadPayloadGivesJustTheEnvelopeFields(string? payload) =>
        Build(payload).EnumerateObject().Select(p => p.Name).Should().BeEquivalentTo("crawlJobId", "eventType", "at");

    [Fact]
    public void KeepsNullFields() =>
        Build("""{"contentType":null}""").GetProperty("contentType").ValueKind.Should().Be(JsonValueKind.Null);
}
