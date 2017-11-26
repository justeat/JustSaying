using System;
using System.Threading.Tasks;
using JustSaying.AwsTools.QueueCreation;
using JustSaying.Messaging;
using JustSaying.Messaging.MessageHandling;
using JustSaying.Messaging.MessageSerialisation;
using JustSaying.Models;
using NSubstitute;
using Xunit;
using Assert = NUnit.Framework.Assert;

namespace JustSaying.UnitTests.JustSayingFluently.AddingHandlers
{
    public class WhenAddingASubscriptionHandler : JustSayingFluentlyTestBase
    {
        private readonly IHandlerAsync<Message> _handler = Substitute.For<IHandlerAsync<Message>>();
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
        public void TheTopicAndQueueIsCreatedInEachRegion()
        {
            QueueVerifier.Received().EnsureTopicExistsWithQueueSubscribed("defaultRegion", Bus.SerialisationRegister, Arg.Any<SqsReadConfiguration>());
            QueueVerifier.Received().EnsureTopicExistsWithQueueSubscribed("failoverRegion", Bus.SerialisationRegister, Arg.Any<SqsReadConfiguration>());
        }

        [Fact]
        public void TheSubscriptionIsCreatedInEachRegion()
        {
            Bus.Received(2).AddNotificationSubscriber(Arg.Any<string>(), Arg.Any<INotificationSubscriber>());
        }

        [Fact]
        public void HandlerIsAddedToBus()
        {
            Bus.Received().AddMessageHandler(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<Func<IHandlerAsync<Message>>>());
        }

        [Fact]
        public void SerialisationIsRegisteredForMessage()
        {
            Bus.SerialisationRegister.Received().AddSerialiser<Message>(Arg.Any<IMessageSerialiser>());
        }

        [Fact]
        public void ICanContinueConfiguringTheBus()
        {
            Assert.IsInstanceOf<IFluentSubscription>(_response);
        }
    }
}
