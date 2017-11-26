using System.Threading.Tasks;
using JustSaying.Models;
using JustSaying.TestingFramework;
using NSubstitute;
using Xunit;

namespace JustSaying.UnitTests.JustSayingFluently.Publishing
{
    public class WhenPublishing : JustSayingFluentlyTestBase
    {
        private readonly Message _message = new GenericMessage();

        protected override void Given(){}

        protected override async Task When()
        {
            await SystemUnderTest.PublishAsync(_message);
        }

        [Fact]
        public void TheMessageIsPublished()
        {
            // If this ever fails, I have serious questions
            Received.InOrder(async () => await Bus.PublishAsync(_message));
        }
    }
}
