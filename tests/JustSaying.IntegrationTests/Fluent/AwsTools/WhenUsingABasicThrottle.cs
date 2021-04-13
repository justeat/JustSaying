using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using JustSaying.AwsTools;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.AwsTools.QueueCreation;
using JustSaying.Messaging.MessageHandling;
using JustSaying.TestingFramework;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;
using Xunit.Abstractions;
#pragma warning disable 618

namespace JustSaying.IntegrationTests.Fluent.AwsTools
{
    public class WhenUsingABasicThrottle : IntegrationTestBase
    {
        public WhenUsingABasicThrottle(ITestOutputHelper outputHelper)
            : base(outputHelper)
        {
        }

        protected override TimeSpan Timeout => TimeSpan.FromMinutes(5);

        [AwsTheory]
        [InlineData(100)]
        [InlineData(1000)]
        public async Task Messages_Are_Throttled_But_Still_Delivered(int throttleMessageCount)
        {
            // Arrange
            ILoggerFactory loggerFactory = OutputHelper.ToLoggerFactory();
            IAwsClientFactory clientFactory = CreateClientFactory();

            int retryCountBeforeSendingToErrorQueue = 1;
            var client = clientFactory.GetSqsClient(Region);

            var queue = new SqsQueueByName(
                Region,
                UniqueName,
                retryCountBeforeSendingToErrorQueue,
                client,
                loggerFactory);

            if (!await queue.ExistsAsync())
            {
                await queue.CreateAsync(new SqsBasicConfiguration());

                if (!IsSimulator)
                {
                    // Wait for up to 60 secs for queue creation to be guaranteed completed by AWS
                    using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1));

                    while (!cts.IsCancellationRequested)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(2));

                        if (await queue.ExistsAsync())
                        {
                            break;
                        }
                    }
                }
            }

            Assert.True(await queue.ExistsAsync(), "The queue was not created.");

            OutputHelper.WriteLine($"{DateTime.Now} - Adding {throttleMessageCount} messages to the queue.");

            var entriesAdded = 0;

            // Add some messages
            do
            {
                var entries = new List<SendMessageBatchRequestEntry>();

                for (int j = 0; j < 10; j++)
                {
                    var batchEntry = new SendMessageBatchRequestEntry
                    {
                        MessageBody = $"{{\"Subject\":\"SimpleMessage\", \"Message\": {{ \"Content\": \"{entriesAdded}\"}}}}",
                        Id = Guid.NewGuid().ToString()
                    };

                    entries.Add(batchEntry);
                    entriesAdded++;
                }

                await client.SendMessageBatchAsync(queue.Uri.AbsoluteUri, entries);
            }
            while (entriesAdded < throttleMessageCount);

            OutputHelper.WriteLine($"{DateTime.Now} - Done adding messages.");

            int count = 0;
            var handler = Substitute.For<IHandlerAsync<SimpleMessage>>();

            handler.Handle(Arg.Any<SimpleMessage>())
                   .ReturnsForAnyArgs(true)
                   .AndDoes((_) => Interlocked.Increment(ref count));

            IServiceCollection services = GivenJustSaying(LogLevel.Warning)
                .ConfigureJustSaying((builder) => builder.WithLoopbackQueue<SimpleMessage>(UniqueName))
                .AddSingleton(handler);

            TimeSpan timeToProcess = TimeSpan.Zero;

            // Act
            await WhenAsync(
                services,
                async (publisher, listener, cancellationToken) =>
                {
                    var stopwatch = Stopwatch.StartNew();
                    var delay = IsSimulator ? TimeSpan.FromMilliseconds(100) : TimeSpan.FromSeconds(5);

                    await listener.StartAsync(cancellationToken);

                    do
                    {
                        await Task.Delay(delay, cancellationToken);

                        OutputHelper.WriteLine($"{DateTime.Now} - Handled {count} messages. Waiting for completion.");
                    }
                    while (count < throttleMessageCount && !cancellationToken.IsCancellationRequested);

                    stopwatch.Stop();
                    timeToProcess = stopwatch.Elapsed;
                });

            // Assert
            OutputHelper.WriteLine($"{DateTime.Now} - Handled {count:N0} messages.");
            OutputHelper.WriteLine($"{DateTime.Now} - Took {timeToProcess.TotalMilliseconds} ms");
            OutputHelper.WriteLine($"{DateTime.Now} - Throughput {(float)count / timeToProcess.TotalMilliseconds * 1000} messages/second");

            Assert.Equal(throttleMessageCount, count);
        }
    }
}
