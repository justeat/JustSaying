using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS.Model;
using Amazon.SQS.Util;
using JustSaying.AwsTools;
using JustSaying.Fluent;
using JustSaying.TestingFramework;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit.Abstractions;

namespace JustSaying.IntegrationTests.Fluent.Subscribing
{
    public class AddressPubSub : IntegrationTestBase
    {
        public AddressPubSub(ITestOutputHelper outputHelper) : base(outputHelper)
        {
        }

        [AwsFact]
        public async Task SimplePubSubWorks()
        {
            IAwsClientFactory clientFactory = CreateClientFactory();
            var sqsClient = clientFactory.GetSqsClient(Region);
            var snsClient = clientFactory.GetSnsClient(Region);
            var queueResponse = await sqsClient.CreateQueueAsync(UniqueName);
            var topicResponse = await snsClient.CreateTopicAsync(UniqueName);
            var subscriptionArn = await snsClient.SubscribeQueueAsync(topicResponse.TopicArn, sqsClient, queueResponse.QueueUrl);

            var handler = new InspectableHandler<SimpleMessage>();

            var services = GivenJustSaying()
                .ConfigureJustSaying((builder) =>
                    builder
                        .Subscriptions(c =>
                            c.ForQueue<SimpleMessage>(QueueAddress.FromUrl(queueResponse.QueueUrl, Region.SystemName)))
                        .Publications(c =>
                            c.WithTopic<SimpleMessage>(TopicAddress.FromArn(topicResponse.TopicArn))
                        )
                )
                .AddJustSayingHandlers(new[] { handler });

            string content = Guid.NewGuid().ToString();

            var message = new SimpleMessage
            {
                Content = content
            };

            await WhenAsync(
                services,
                async (publisher, listener, serviceProvider, cancellationToken) =>
                {

                    await listener.StartAsync(cancellationToken);
                    await publisher.StartAsync(cancellationToken);

                    await publisher.PublishAsync(message, cancellationToken);

                    // Assert
                    await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);

                    handler.ReceivedMessages.ShouldHaveSingleItem().Content.ShouldBe(content);
                });
        }
    }
}
