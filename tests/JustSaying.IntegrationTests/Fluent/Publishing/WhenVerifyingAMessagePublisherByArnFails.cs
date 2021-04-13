using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SimpleNotificationService.Model;
using JustSaying.Fluent;
using JustSaying.IntegrationTests.Fluent.Subscribing;
using JustSaying.Messaging;
using JustSaying.Naming;
using JustSaying.TestingFramework;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace JustSaying.IntegrationTests.Fluent.Publishing
{
    public class WhenVerifyingAMessagePublisherByArnFails : IntegrationTestBase
    {
        public WhenVerifyingAMessagePublisherByArnFails(ITestOutputHelper outputHelper)
            : base(outputHelper)
        {
        }

        [AwsFact]
        public async Task Then_The_Topic_Does_Not_Exist()
        {
            // Arrange
            string topicName = new UniqueTopicNamingConvention().AsArn<SimpleMessage>();

            // Act - Check topic creation
            var createdOK = true;
            try
            {
                //Create Bus that uses this topic
                var serviceProvider = GivenJustSaying()
                    .ConfigureJustSaying(
                        builder => builder.Publications(
                            options =>
                            {
                                options.WithQueue<SimpleMessage>(UniqueName);
                                options.WithTopic<SimpleMessage>(configure =>
                                {
                                    //The topic should already exists and we just want to verify it
                                    configure
                                        .WithTopic(topicARN: topicName)
                                        .WithInfastructure(InfrastructureAction.ValidateExists);
                                });
                            })
                    )
                    .BuildServiceProvider();

                // Act - Force startup tasks to run
                IMessagePublisher publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
                await publisher.StartAsync(CancellationToken.None);

            }
            catch (InvalidOperationException)
            {
                createdOK = false;
            }

            // Assert
            createdOK.ShouldBeFalse();

        }
    }
}
