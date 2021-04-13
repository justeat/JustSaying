using System;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS.Model;
using JustSaying.Fluent;
using JustSaying.TestingFramework;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit.Abstractions;

namespace JustSaying.IntegrationTests.Fluent.Subscribing
{
    public class WhenExplicityCreatingAMessageSubscriberForTopic : IntegrationTestBase
    {
        public WhenExplicityCreatingAMessageSubscriberForTopic (ITestOutputHelper outputHelper)
            : base(outputHelper)
        {
        }

        [AwsFact]
        public async Task Then_The_Topic_Exist()
        {
            // Arrange
            var topicName = new UniqueTopicNamingConvention().TopicName<SimpleMessage>();

            // Act - Force topic creation
            var createdOK = true;
            try
            {
                var completionSource = new TaskCompletionSource<object>();
                var handler = CreateHandler<SimpleMessage>(completionSource);
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
                                        .WithInfrastructure(InfrastructureAction.CreateIfMissing);
                                });
                           });
                        })
                    .AddSingleton(handler)
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
            createdOK.ShouldBeTrue();
            await ValidateRequiredInfrastructure(topicName, UniqueName);

        }

        private async Task ValidateRequiredInfrastructure(string topicName, string QueueName)
        {
            var snsClient = CreateClientFactory().GetSnsClient(Region);
            var findTopicResult = await snsClient.FindTopicAsync(topicName);
            findTopicResult.TopicArn.ShouldNotBeNull();


            //and now the required queue
            var sqsClient = CreateClientFactory().GetSqsClient(Region);
            var findQueueResult = await sqsClient.GetQueueUrlAsync(UniqueName);
            findQueueResult.HttpStatusCode.ShouldBe(HttpStatusCode.OK);
            findQueueResult.QueueUrl.ShouldNotBeNull();

            //now find the binding
            var queueAttributesib = await sqsClient.GetQueueAttributesAsync(new GetQueueAttributesRequest{QueueUrl = findQueueResult.QueueUrl});
            var queueArn = queueAttributesib.QueueARN;

            bool exists = false;
            ListSubscriptionsByTopicResponse response;
            do
            {
                response = await snsClient.ListSubscriptionsByTopicAsync(new ListSubscriptionsByTopicRequest {TopicArn =findTopicResult.TopicArn});
                exists = response.Subscriptions.Any(sub => (sub.Protocol.ToLower() == "sqs") && (sub.Endpoint == queueArn));
            } while (!exists && response.NextToken != null);

            exists.ShouldBeTrue();

        }
    }
}
