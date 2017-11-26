using System;
using System.Threading.Tasks;
using JustSaying.Messaging.MessageHandling;
using JustSaying.Models;
using NSubstitute;
using Xunit;

namespace JustSaying.UnitTests.JustSayingFluently.AddingHandlers
{
    public class JustSayingMessage<T> : Message
    {
        public T Contents { get; set; }
    }

    public class MyMessage { }

    public class WhenAddingASubscriptionHandlerForAGenericMessage : JustSayingFluentlyTestBase
    {
        private readonly IHandlerAsync<JustSayingMessage<MyMessage>> _handler = Substitute.For<IHandlerAsync<JustSayingMessage<MyMessage>>>();
        private object _response;

        protected override void Given()
        {
        }

        protected override Task When()
        {
            _response = SystemUnderTest
                .WithSqsTopicSubscriber()
                .IntoDefaultQueue()
                .ConfigureSubscriptionWith(cfg => { })
                .WithMessageHandler(_handler);

            return Task.CompletedTask;
        }

        [Fact]
        public void HandlerIsAddedToBus()
        {
            Bus.Received().AddMessageHandler(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<Func<IHandlerAsync<JustSayingMessage<MyMessage>>>>());
        }
    }
}
