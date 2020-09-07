using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS;
using Amazon.SQS.Model;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.Messaging.MessageProcessingStrategies;
using JustSaying.TestingFramework;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace JustSaying.UnitTests.Messaging.Channels.SubscriptionGroupTests
{
    public class WhenListeningStartsAndStops : BaseSubscriptionGroupTests
    {
        private const string AttributeMessageContentsRunning = @"Message Contents Running";
        private const string AttributeMessageContentsAfterStop = @"Message Contents After Stop";

        private int _expectedMaxMessageCount;
        private bool _running;
        private FakeSqsQueue _queue;
        private FakeAmazonSqs _client;

        public WhenListeningStartsAndStops(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        {
        }

        protected override void Given()
        {
            // we expect to get max 10 messages per batch
            _expectedMaxMessageCount = MessageDefaults.MaxAmazonMessageCap;

            Logger.LogInformation("Expected max message count is {MaxMessageCount}", _expectedMaxMessageCount);

            var response1 = new List<Message> { new Message { Body = AttributeMessageContentsRunning } };
            var response2 = new List<Message> { new Message { Body = AttributeMessageContentsAfterStop } };

            _queue = CreateSuccessfulTestQueue("TestQueue", () => _running ? response1 : response2);
            _client = _queue.FakeClient;

            Queues.Add(_queue);
        }

        protected override async Task WhenAsync()
        {
            _running = true;
            var cts = new CancellationTokenSource();
            var completion = SystemUnderTest.RunAsync(cts.Token);
            cts.CancelAfter(TimeSpan.FromSeconds(2));

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => completion);
            _running = false;
        }

        [Fact]
        public void MessagesAreReceived()
        {
            _client.ReceiveMessageRequests.ShouldNotBeEmpty();
        }

        [Fact]
        public void TheMaxMessageAllowanceIsGrabbed()
        {
            _client.ReceiveMessageRequests.ShouldAllBe(req => req.MaxNumberOfMessages == _expectedMaxMessageCount);
        }

        [Fact]
        public void MessageIsProcessed()
        {
            SerializationRegister.ReceivedDeserializationRequests.ShouldContain(AttributeMessageContentsRunning);
            SerializationRegister.ReceivedDeserializationRequests.ShouldNotContain(AttributeMessageContentsAfterStop);
        }
    }
}
