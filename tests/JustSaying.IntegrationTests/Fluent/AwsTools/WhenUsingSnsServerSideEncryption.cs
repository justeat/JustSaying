using System;
using System.Threading.Tasks;
using JustSaying.AwsTools;
using JustSaying.AwsTools.QueueCreation;
using JustSaying.Messaging;
using JustSaying.TestingFramework;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Xunit.Abstractions;

namespace JustSaying.IntegrationTests.Fluent.AwsTools
{
    public class WhenUsingSnsServerSideEncryption : IntegrationTestBase
    {
        public WhenUsingSnsServerSideEncryption(ITestOutputHelper outputHelper)
            : base(outputHelper)
        {
        }

        [NotSimulatorFact]
        public async Task Then_The_Message_Is_Published()
        {
            // Arrange
            var completionSource = new TaskCompletionSource<object>();
            var handler = CreateHandler<SimpleMessage>(completionSource);

            string masterSnsKeyId = JustSayingConstants.DefaultSnsAttributeEncryptionKeyId;

            var services = GivenJustSaying()
                .ConfigureJustSaying(
                    (builder) => builder.Publications((options) => options.WithQueue<SimpleMessage>(
                        (queue) => queue.WithWriteConfiguration(
                            (config) => config.WithQueueName(UniqueName)))
                        .WithTopic<SimpleMessage>(topic => topic.WithWriteConfiguration(writeConfig => writeConfig.Encryption = new ServerSideEncryption { KmsMasterKeyId = masterSnsKeyId }))))
                .ConfigureJustSaying(
                    (builder) => builder.Subscriptions((options) => options.ForTopic<SimpleMessage>(topic => topic.WithTopic(UniqueName))))
                .AddSingleton(handler);

            string content = Guid.NewGuid().ToString();

            var message = new SimpleMessage
            {
                Content = content
            };

            await WhenAsync(
                services,
                async (publisher, listener, cancellationToken) =>
                {
                    await listener.StartAsync(cancellationToken);

                    // Act
                    await publisher.PublishAsync(message, cancellationToken);

                    // Assert
                    completionSource.Task.Wait(cancellationToken);

                    await handler.Received().Handle(Arg.Is<SimpleMessage>((m) => m.Content == content));
                });
        }
    }
}
