using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS.Model;
using JustSaying.Fluent;
using JustSaying.Naming;
using JustSaying.TestingFramework;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit.Abstractions;

namespace JustSaying.IntegrationTests.Fluent.Subscribing
{
    public class WhenVerifyingAMessageSubcriber : IntegrationTestBase
    {
        public WhenVerifyingAMessageSubcriber(ITestOutputHelper outputHelper)
            : base(outputHelper)
        {
        }

        [AwsFact]
        public async Task Then_The_Bus_Fails_Fast()
        {
            // Arrange
            var topicName = new UniqueTopicNamingConvention().TopicName<SimpleMessage>();
            await CreateRequiredInfrastructure();

            // Act - Force topic creation
            var createdOK = true;
            try
            {
                //Create Bus that uses this topic
                var serviceProvider = GivenJustSaying()
                    .ConfigureJustSaying(
                        builder =>
                        {
                            builder.Subscriptions(options =>
                            {
                                options.ForTopic<SimpleMessage>(topicConfig =>
                                {
                                    topicConfig
                                        .WithQueue(UniqueName)
                                        .WithTopic(topicName)
                                        .WithInfrastructure(InfrastructureAction.ValidateExists);
                                });
                            });
                        })
                    .BuildServiceProvider();

                IMessagingBus listener = serviceProvider.GetRequiredService<IMessagingBus>();
                var ctx = new CancellationTokenSource();

                await listener.StartAsync(ctx.Token);

                ctx.CancelAfter(1000);
            }
            catch (InvalidOperationException)
            {
                createdOK = false;
            }

            // Assert
            createdOK.ShouldBeFalse();

        }

        private async Task CreateRequiredInfrastructure()
        {
            var sqsClient = CreateClientFactory().GetSqsClient(Region);
            var createQueueResponse = await sqsClient.CreateQueueAsync(new CreateQueueRequest()
            {
                QueueName = UniqueName,
                Tags = new Dictionary<string, string> { { "Author", "WhenVerifyingAMessagePublisher" } }
            });

            //sanity check
            createQueueResponse.HttpStatusCode.ShouldBe(HttpStatusCode.OK);
        }
    }
}
