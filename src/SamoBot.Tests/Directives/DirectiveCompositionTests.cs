using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using SamoBot.Core.Directives;
using SamoBot.Core.Directives.Rules;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Directives;

public class DirectiveCompositionTests
{
    [Fact]
    public void TheFetchersKeepExactlyTheHeadersTheRegisteredSourcesRead() =>
        TestDirectives.Headers.Names.Should().BeEquivalentTo("X-Robots-Tag", "Link");

    [Fact]
    public void ADirectiveIsRegisteredUnderEveryDirectiveInterfaceItImplements()
    {
        var provider = new ServiceCollection().AddCrawlDirectives().BuildServiceProvider();

        provider.GetServices<IPagePolicyRule>().Should().Contain(rule => rule is NonCanonicalPageRule);
        provider.GetServices<IRedirectSource>().Should().HaveCount(2);
        provider.GetServices<ICanonicalSource>().Should().HaveCount(2);
        provider.GetServices<IRobotsDirectiveSource>().Should().HaveCount(2);
        provider.GetServices<ILinkFollowRule>().Should().ContainSingle();
    }

    [Fact]
    public void AnObjectThatIsNoDirectiveIsRejected() =>
        FluentActions.Invoking(() => new ServiceCollection().AddDirective(new object())).Should().Throw<ArgumentException>();
}
