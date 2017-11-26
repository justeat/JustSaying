using System.Threading.Tasks;
using Amazon.SimpleNotificationService.Model;
using JustSaying.Models;
using NUnit.Framework;
using Xunit;
using Assert = Xunit.Assert;

namespace JustSaying.IntegrationTests.WhenRegisteringAPublisher
{
    public class WhenRegisteringAPublisherInANonDefaultRegion : FluentNotificationStackTestBase
    {
        private string _topicName;
        private Topic _topic;

        protected override void Given()
        {
            base.Given();

            _topicName = "message";

            Configuration = new MessagingConfig();

            DeleteTopicIfItAlreadyExists(TestEndpoint, _topicName).Wait();

        }

        protected override Task When()
        {
            SystemUnderTest.WithSnsMessagePublisher<Message>();
            return Task.CompletedTask;
        }

        [Fact]
        public async Task ASnsTopicIsCreatedInTheNonDefaultRegion()
        {
            bool topicExists;
            (topicExists, _topic) = await TryGetTopic(TestEndpoint, _topicName);
            Assert.True(topicExists);
        }

        [TearDown]
        public void TearDown()
        {
            if (_topic != null)
            {
                DeleteTopic(TestEndpoint, _topic).Wait();
            }
        }
    }
}
