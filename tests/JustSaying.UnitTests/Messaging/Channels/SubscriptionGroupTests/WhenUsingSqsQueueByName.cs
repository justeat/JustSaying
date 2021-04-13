using System;
using System.Collections.Generic;
using System.Linq;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.TestingFramework;
using Shouldly;
using Xunit;
using Xunit.Abstractions;
#pragma warning disable 618

namespace JustSaying.UnitTests.Messaging.Channels.SubscriptionGroupTests
{
    public sealed class WhenUsingSqsQueueByName : BaseSubscriptionGroupTests, IDisposable
    {
        private ISqsQueue _queue;
        private IAmazonSQS _client;
        readonly string MessageTypeString = typeof(SimpleMessage).ToString();
        const string MessageBody = "object";

        public WhenUsingSqsQueueByName(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        { }

        protected override void Given()
        {
            int retryCount = 1;

            _client = new FakeAmazonSqs(() =>
            {
                return new[] { GenerateResponseMessages(MessageTypeString, Guid.NewGuid()) }
                    .Concat(new ReceiveMessageResponse().Infinite());
            });

            var queue = new SqsQueueByName(RegionEndpoint.EUWest1,
                "some-queue-name",
                retryCount,
                _client,
                LoggerFactory);
            queue.ExistsAsync().Wait();

            _queue = queue;

            Queues.Add(_queue);
        }

        [Fact]
        public void HandlerReceivesMessage()
        {
            Handler.ReceivedMessages.Contains(SerializationRegister.DefaultDeserializedMessage())
                .ShouldBeTrue();
        }

        private static ReceiveMessageResponse GenerateResponseMessages(
            string messageType,
            Guid messageId)
        {
            return new ReceiveMessageResponse
            {
                Messages = new List<Message>
                {
                    new Message
                    {
                        MessageId = messageId.ToString(),
                        Body = SqsMessageBody(messageType)
                    },
                    new Message
                    {
                        MessageId = messageId.ToString(),
                        Body = "{\"Subject\":\"SOME_UNKNOWN_MESSAGE\"," +
                            "\"Message\":\"SOME_RANDOM_MESSAGE\"}"
                    }
                }
            };
        }

        private static string SqsMessageBody(string messageType)
        {
            return "{\"Subject\":\"" + messageType + "\"," + "\"Message\":\"" + MessageBody + "\"}";
        }

        public void Dispose()
        {
            _client.Dispose();
        }
    }
}
