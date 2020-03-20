using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.AwsTools.MessageHandling.Dispatch;
using JustSaying.Messaging.Channels;
using JustSaying.Messaging.Channels.Configuration;
using JustSaying.Messaging.Channels.ConsumerGroups;
using JustSaying.Messaging.Channels.Dispatch;
using JustSaying.Messaging.Channels.Multiplexer;
using JustSaying.Messaging.Channels.Receive;
using JustSaying.Messaging.Monitoring;
using JustSaying.UnitTests.Messaging.Channels;
using JustSaying.UnitTests.Messaging.Channels.TestHelpers;
using JustSaying.UnitTests.Messaging.Policies.ExamplePolicies;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace JustSaying.UnitTests.Messaging.Policies
{
    public class ChannelPolicyTests
    {
        private ILoggerFactory LoggerFactory { get; }
        private IMessageMonitor MessageMonitor { get; }


        public ChannelPolicyTests(ITestOutputHelper testOutputHelper)
        {
            LoggerFactory = testOutputHelper.ToLoggerFactory();
            MessageMonitor = new LoggingMonitor(LoggerFactory.CreateLogger<IMessageMonitor>());
        }

        private static readonly TimeSpan TimeoutPeriod = TimeSpan.FromMilliseconds(100);

        [Fact]
        public async Task ErrorHandlingAroundSqs()
        {
            // Arrange
            int queueCalledCount = 0;
            int dispatchedMessageCount = 0;
            var sqsQueue = TestQueue(() => Interlocked.Increment(ref queueCalledCount));

            var queues = new List<ISqsQueue> { sqsQueue };

            var config = new ConsumerGroupConfig();
            config.WithSqsPolicy(
                next =>
                    new ErrorHandlingMiddleware<GetMessagesContext, IList<Message>, InvalidOperationException>(next));
            var consumerGroupSettings = config.CreateConsumerGroupSettings(queues);
            var settings = new Dictionary<string, ConsumerGroupSettings>
            {
                { "test", consumerGroupSettings },
            };

            IMessageDispatcher dispatcher = new FakeDispatcher(() => Interlocked.Increment(ref dispatchedMessageCount));

            var receiveBufferFactory = new ReceiveBufferFactory(LoggerFactory, config, MessageMonitor);
            var multiplexerFactory = new MultiplexerFactory(LoggerFactory);
            var consumerFactory = new ChannelDispatcherFactory(dispatcher);
            var consumerBusFactory = new SingleConsumerGroupFactory(multiplexerFactory, receiveBufferFactory, consumerFactory, LoggerFactory);

            var bus = new CombinedConsumerGroup(
                consumerBusFactory,
                settings,
                LoggerFactory.CreateLogger<CombinedConsumerGroup>());

            var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeoutPeriod);

            // Act and Assert
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => bus.Run(cts.Token));

            queueCalledCount.ShouldBeGreaterThan(1);
            dispatchedMessageCount.ShouldBe(0);
        }

        private static ISqsQueue TestQueue(Action spy = null)
        {
            async Task<IList<Message>> GetMessages()
            {
                await Task.Delay(5).ConfigureAwait(false);
                spy?.Invoke();
                throw new InvalidOperationException();
            }

            ISqsQueue sqsQueueMock = Substitute.For<ISqsQueue>();
            sqsQueueMock
                .GetMessagesAsync(Arg.Any<int>(), Arg.Any<List<string>>(), Arg.Any<CancellationToken>())
                .Returns(async _ => await GetMessages());

            return sqsQueueMock;
        }
    }
}
