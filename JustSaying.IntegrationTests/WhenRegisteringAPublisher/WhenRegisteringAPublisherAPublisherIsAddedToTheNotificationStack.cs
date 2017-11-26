using System.Threading.Tasks;
using JustSaying.Messaging;
using JustSaying.Messaging.MessageSerialisation;
using JustSaying.Models;
using NSubstitute;
using NUnit.Framework;
using Xunit;

namespace JustSaying.IntegrationTests.WhenRegisteringAPublisher
{
    public class WhenRegisteringAPublisher : FluentNotificationStackTestBase
    {
        private string _topicName;

        protected override void Given()
        {
            base.Given();

            _topicName = "CustomerCommunication";

            EnableMockedBus();

            Configuration = new MessagingConfig();

            DeleteTopicIfItAlreadyExists(TestEndpoint, _topicName).Wait();
        }

        protected override Task When()
        {
            SystemUnderTest.WithSnsMessagePublisher<Message>();
            return Task.CompletedTask;
        }

        [Fact]
        public void APublisherIsAddedToTheStack()
        {
            NotificationStack.Received().AddMessagePublisher<Message>(Arg.Any<IMessagePublisher>(), TestEndpoint.SystemName);
        }

        [Fact]
        public void SerialisationIsRegisteredForMessage()
        {
            NotificationStack.SerialisationRegister.Received()
                .AddSerialiser<Message>(Arg.Any<IMessageSerialiser>());
        }

        [TearDown]
        public void TearDown()
        {
            DeleteTopicIfItAlreadyExists(TestEndpoint, _topicName).Wait();
        }
    }
}
