using System;
using System.Threading.Tasks;
using Amazon.SQS.Model;
using JustSaying.IntegrationTests.TestHandlers;
using JustSaying.Messaging;
using JustSaying.Messaging.MessageHandling;
using JustSaying.Messaging.Monitoring;
using JustSaying.TestingFramework;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace JustSaying.IntegrationTests.Fluent.Subscribing
{
    public class WhenAHandlerThrowsAnExceptionWithNoMonitor : IntegrationTestBase
    {
        public WhenAHandlerThrowsAnExceptionWithNoMonitor(ITestOutputHelper outputHelper)
            : base(outputHelper)
        {
        }

        [AwsFact]
        public async Task Then_The_Message_Is_Handled()
        {
            // Arrange
            var monitor = new TrackingLoggingMonitor(NullLogger<TrackingLoggingMonitor>.Instance);

            var handler = new ThrowingHandler();

            var services = GivenJustSaying()
                .ConfigureJustSaying((builder) => builder.Publications((options) =>
                    options.WithQueue<SimpleMessage>(UniqueName)))
                .ConfigureJustSaying(
                    (builder) => builder.Subscriptions(
                        (options) => options.ForQueue<SimpleMessage>(
                            (queue) => queue.WithQueue(UniqueName)))
                        .Services(c => c.WithMessageMonitoring(() => monitor)))
                .AddSingleton<IHandlerAsync<SimpleMessage>>(handler);

            var message = new SimpleMessage();

            await WhenAsync(
                services,
                async (publisher, listener, cancellationToken) =>
                {
                    await listener.StartAsync(cancellationToken);
                    await publisher.StartAsync(cancellationToken);

                    // Act
                    await publisher.PublishAsync(message, cancellationToken);
                    await handler.DoneSignal.Task;

                    // Assert
                    handler.MessageReceived.ShouldNotBeNull();
                    monitor.HandledErrors.Count.ShouldBe(1);
                });
        }
    }
}
