using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.TestingFramework;
using NSubstitute;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace JustSaying.UnitTests.Messaging.Channels.SubscriptionGroupTests
{
    public class WhenThereAreExceptionsInSqsCalling : BaseSubscriptionGroupTests
    {
        private ISqsQueue _queue;
        private int _callCount;

        public WhenThereAreExceptionsInSqsCalling(ITestOutputHelper testOutputHelper)
            : base(testOutputHelper)
        {
        }

        protected override void Given()
        {
            _queue = CreateSuccessfulTestQueue("TestQueue", ExceptionOnFirstCall);
            Queues.Add(_queue);

            SerializationRegister.DefaultDeserializedMessage =
                () => throw new TestException("Test from WhenThereAreExceptionsInMessageProcessing");
        }

        private IEnumerable<Message> ExceptionOnFirstCall()
        {
            _callCount++;
            if (_callCount == 1)
            {
                throw new TestException("testing the failure on first call");
            }

            return new List<Message>();
        }

        protected override async Task WhenAsync()
        {
            var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeoutPeriod);

            var completion = SystemUnderTest.RunAsync(cts.Token);

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => completion);
        }

        [Fact]
        public void QueueIsPolledMoreThanOnce()
        {
            _callCount.ShouldBeGreaterThan(1);
        }
    }
}
