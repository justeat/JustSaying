using JustBehave;
using JustSaying.Messaging.MessageHandling;
using JustSaying.Models;
using NSubstitute;

namespace JustSaying.UnitTests.JustSayingFluently.AddingHandlers
{
    public class WhenAddingASubscriptionHandlerWithoutCustomConfig : JustSayingFluentlyTestBase
    {
        private readonly IHandlerAsync<Message> _handler = Substitute.For<IHandlerAsync<Message>>();
        private IFluentSubscription _bus;

        protected override void Given() { }

        protected override void When()
        {
            _bus = SystemUnderTest
                .WithSqsTopicSubscriber()
                .IntoQueueNamed("queuename");
        }

        [Then]
        public void ConfigurationIsNotRequired()
        {
            // Tested by the fact that handlers can be added
            _bus.WithMessageHandler(_handler)
                .WithMessageHandler(_handler);
        }

        [Then]
        public void ConfigurationCanBeProvided()
        {
            _bus.ConfigureSubscriptionWith(conf => conf.InstancePosition = 1);
        }
    }
}