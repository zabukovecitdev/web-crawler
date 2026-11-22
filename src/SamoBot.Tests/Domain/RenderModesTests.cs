using FluentAssertions;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Domain;

public class RenderModesTests
{
    [Theory]
    [InlineData("Auto", null, RenderMode.Auto)]
    [InlineData("always", null, RenderMode.Always)]
    [InlineData("NEVER", true, RenderMode.Never)]
    [InlineData(null, true, RenderMode.Always)]
    [InlineData(null, false, RenderMode.Never)]
    [InlineData(null, null, RenderMode.Auto)]
    [InlineData("sometimes", true, RenderMode.Always)]
    [InlineData("7", null, RenderMode.Auto)]
    public void TheModeComesFromTheRequestAndFallsBackToTheOldSwitch(string? mode, bool? legacy, RenderMode expected) =>
        RenderModes.FromRequest(mode, legacy).Should().Be(expected);

    [Theory]
    [InlineData(RenderMode.Always, false, true)]
    [InlineData(RenderMode.Always, true, true)]
    [InlineData(RenderMode.Auto, true, true)]
    [InlineData(RenderMode.Auto, false, false)]
    [InlineData(RenderMode.Never, true, false)]
    [InlineData(RenderMode.Never, false, false)]
    public void OnlyAlwaysAndAutoOnAJsSiteNeedTheBrowser(RenderMode mode, bool jsNeeded, bool expected) =>
        RenderModes.NeedsBrowser(mode, jsNeeded).Should().Be(expected);
}
