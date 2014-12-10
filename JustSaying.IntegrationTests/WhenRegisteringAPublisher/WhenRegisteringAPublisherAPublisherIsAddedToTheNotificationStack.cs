using JustBehave;
using JustSaying.Messaging;
using JustSaying.Messaging.MessageSerialisation;
using JustSaying.Models;
using NSubstitute;
using NUnit.Framework;

namespace JustSaying.IntegrationTests.WhenRegisteringAPublisher
{
    public class WhenRegisteringAPublisher : FluentNotificationStackTestBase
    {
        private string _topicName;

        protected override void Given()
        {
            _topicName = "CustomerCommunication";

            EnableMockedBus();

            Configuration = new MessagingConfig();

            DeleteTopicIfItAlreadyExists(TestEndpoint, _topicName);
        }

        protected override void When()
        {
            SystemUnderTest.WithSnsMessagePublisher<Message>();
        }

        [Then]
        public void APublisherIsAddedToTheStack()
        {
            NotificationStack.Received().AddMessagePublisher<Message>(Arg.Any<IMessagePublisher>(), TestEndpoint.SystemName);
        }

        [Then]
        public void SerialisationIsRegisteredForMessage()
        {
            NotificationStack.SerialisationRegister.Received()
                .AddSerialiser<Message>(Arg.Any<IMessageSerialiser<Message>>());
        }

        [TearDown]
        public void TearDown()
        {
            DeleteTopicIfItAlreadyExists(TestEndpoint, _topicName);
        }
    }
}