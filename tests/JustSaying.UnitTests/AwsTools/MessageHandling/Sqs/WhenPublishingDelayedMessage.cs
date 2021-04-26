using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon;
using Amazon.SQS.Model;
using JustSaying.Messaging;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.Messaging.MessageSerialization;
using JustSaying.TestingFramework;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace JustSaying.UnitTests.AwsTools.MessageHandling.Sqs
{
    public class WhenPublishingDelayedMessage : WhenPublishingTestBase
    {
        private readonly IMessageSerializationRegister _serializationRegister = Substitute.For<IMessageSerializationRegister>();
        private const string Url = "https://testurl.com/" + QueueName;

        private readonly SimpleMessage _message = new SimpleMessage();
        private readonly PublishMetadata _metadata = new PublishMetadata
        {
            Delay = TimeSpan.FromSeconds(1)
        };

        private const string QueueName = "queuename";

        private protected override Task<SqsMessagePublisher> CreateSystemUnderTestAsync()
        {
            var sqs = new SqsMessagePublisher(new Uri(Url), Sqs, _serializationRegister, Substitute.For<ILoggerFactory>());
            return Task.FromResult(sqs);
        }

        protected override void Given()
        {
            Sqs.ListQueuesAsync(Arg.Any<ListQueuesRequest>()).Returns(new ListQueuesResponse { QueueUrls = new List<string> { Url } });
            Sqs.GetQueueAttributesAsync(Arg.Any<GetQueueAttributesRequest>()).Returns(new GetQueueAttributesResponse());
        }

        protected override async Task WhenAsync()
        {
            await SystemUnderTest.PublishAsync(_message, _metadata);
        }

        [Fact]
        public void MessageIsPublishedWithDelaySecondsPropertySet()
        {
            Sqs.Received().SendMessageAsync(Arg.Is<SendMessageRequest>(x => x.DelaySeconds.Equals(1)));
        }
    }
}
