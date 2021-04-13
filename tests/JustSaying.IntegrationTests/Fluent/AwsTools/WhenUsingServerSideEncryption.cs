using System;
using System.Linq;
using System.Threading.Tasks;
using JustSaying.Messaging;
using JustSaying.Messaging.MessageHandling;
using JustSaying.TestingFramework;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Shouldly;
using Xunit.Abstractions;

namespace JustSaying.IntegrationTests.Fluent.AwsTools
{
    public class WhenUsingServerSideEncryption : IntegrationTestBase
    {
        public WhenUsingServerSideEncryption(ITestOutputHelper outputHelper)
            : base(outputHelper)
        {
        }

        [AwsFact]
        public async Task Then_The_Message_Is_Handled()
        {
            // Arrange
            var handler = new InspectableHandler<SimpleMessage>();

            string masterKeyId = "alias/aws/sqs";

            var services = GivenJustSaying()
                .ConfigureJustSaying(
                    (builder) => builder.Publications((options) => options.WithQueue<SimpleMessage>(
                        (queue) => queue.WithWriteConfiguration(
                            (config) => config.WithQueueName(UniqueName).WithEncryption(masterKeyId)))))
                .ConfigureJustSaying(
                    (builder) => builder.Subscriptions((options) => options.ForQueue<SimpleMessage>(
                        (queue) => queue.WithQueue(UniqueName).WithReadConfiguration(
                            (config) => config.WithEncryption(masterKeyId)))))
                .AddSingleton<IHandlerAsync<SimpleMessage>>(handler);

            string content = Guid.NewGuid().ToString();

            var message = new SimpleMessage()
            {
                Content = content
            };

            await WhenAsync(
                services,
                async (publisher, listener, cancellationToken) =>
                {
                    await listener.StartAsync(cancellationToken);
                    await publisher.StartAsync(cancellationToken);

                    // Act
                    await publisher.PublishAsync(message, cancellationToken);

                    // Assert
                    await Patiently.AssertThatAsync(OutputHelper,
                        () => handler.ReceivedMessages.Any(msg => msg.Content == content));
                });
        }
    }
}
