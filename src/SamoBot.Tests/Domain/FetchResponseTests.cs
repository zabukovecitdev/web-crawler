using FluentAssertions;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Domain;

public class FetchResponseTests
{
    private static FetchResponse Response(string? contentType, string body) => new()
    {
        StatusCode = 200,
        ContentType = contentType,
        Content = System.Text.Encoding.UTF8.GetBytes(body)
    };

    [Theory]
    [InlineData("text/html")]
    [InlineData("text/html; charset=utf-8")]
    [InlineData("TEXT/HTML")]
    [InlineData("application/xhtml+xml")]
    public void IsHtmlWhenTheMediaTypeIsHtmlReturnsTrue(string contentType)
    {
        Response(contentType, "plain").IsHtml.Should().BeTrue();
    }

    [Theory]
    [InlineData(null, "<!DOCTYPE html><title>x</title>")]
    [InlineData("application/octet-stream", "  <html lang=en>")]
    public void IsHtmlWhenTheMediaTypeIsMissingOrWrongLooksAtTheFirstBytes(string? contentType, string body)
    {
        Response(contentType, body).IsHtml.Should().BeTrue();
    }

    [Theory]
    [InlineData("application/json", "{\"a\":1}")]
    [InlineData(null, "")]
    [InlineData("text/plain", "just text")]
    public void IsHtmlForOtherContentReturnsFalse(string? contentType, string body)
    {
        Response(contentType, body).IsHtml.Should().BeFalse();
    }

    [Theory]
    [InlineData("text/markdown; charset=utf-8", true)]
    [InlineData("text/x-markdown", true)]
    [InlineData("text/html", true)]
    [InlineData("text/plain", true)]
    [InlineData("application/json", false)]
    [InlineData("application/pdf", false)]
    public void IsStoredTypeIsTrueForHtmlMarkdownAndPlainText(string contentType, bool expected)
    {
        Response(contentType, "# Title").IsStoredType.Should().Be(expected);
    }

    [Theory]
    [InlineData(199, false)]
    [InlineData(200, true)]
    [InlineData(299, true)]
    [InlineData(300, false)]
    [InlineData(0, false)]
    public void IsSuccessIsTrueOnlyFor2xx(int status, bool expected)
    {
        new FetchResponse { StatusCode = status }.IsSuccess.Should().Be(expected);
    }

    [Theory]
    [InlineData("text/html; charset=utf-8", "<p>x</p>", "text/html; charset=utf-8")]
    [InlineData("text/plain", "<!doctype html><html>", "text/html")]
    [InlineData("text/markdown", "Use `<html lang=en>` at the top.", "text/markdown")]
    [InlineData(null, "<!DOCTYPE html>", "text/html")]
    [InlineData("application/octet-stream", "<html>", "text/html")]
    [InlineData("text/markdown; charset=utf-8", "# Title", "text/markdown; charset=utf-8")]
    [InlineData("text/plain", "just text", "text/plain")]
    public void StoredContentTypeIsHtmlWheneverTheReadersTreatTheBodyAsHtml(string? contentType, string body, string expected)
    {
        Response(contentType, body).StoredContentType.Should().Be(expected);
    }
}
