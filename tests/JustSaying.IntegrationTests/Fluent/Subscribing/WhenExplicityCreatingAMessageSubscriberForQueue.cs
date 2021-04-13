using System;
using System.Collections.Generic;
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
    public class WhenExplicityCreatingAMessageSubscriberForQueue : IntegrationTestBase
    {
        public WhenExplicityCreatingAMessageSubscriberForQueue(ITestOutputHelper outputHelper)
            : base(outputHelper)
        {
        }

        [AwsFact]
        public async Task Then_The_Topic_Exist()
        {
            // Arrange
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
                                options.ForQueue<SimpleMessage>(queueConfig =>
                                 {
                                     queueConfig
                                         .WithQueue(UniqueName)
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
            var sqsClient = CreateClientFactory().GetSqsClient(Region);
            var findQueueResult = await sqsClient.GetQueueUrlAsync(UniqueName);
            findQueueResult.HttpStatusCode.ShouldBe(HttpStatusCode.OK);
            findQueueResult.QueueUrl.ShouldNotBeNull();
        }
   }
}
