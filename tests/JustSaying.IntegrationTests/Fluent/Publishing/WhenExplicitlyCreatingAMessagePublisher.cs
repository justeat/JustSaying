using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.SimpleNotificationService.Model;
using JustSaying.Fluent;
using JustSaying.Messaging;
using JustSaying.Naming;
using JustSaying.TestingFramework;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;
using Xunit.Abstractions;

namespace JustSaying.IntegrationTests.Fluent.Publishing
{
    public class WhenExplicitlyCreatingAMessagePublisher : IntegrationTestBase
    {
        public WhenExplicitlyCreatingAMessagePublisher(ITestOutputHelper outputHelper)
            : base(outputHelper)
        {
        }

        [AwsFact]
        public async Task Then_The_Topic_Exist()
        {
            // Arrange
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
                                configure.WithInfastructure(InfrastructureAction.CreateIfMissing);
                            });
                        })
                )
                .BuildServiceProvider();


            // Act - Force topic creation
            IMessagePublisher publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
            await publisher.StartAsync(CancellationToken.None);


            // Assert
            var topicCreated = await FindTopicByName<SimpleMessage>();
            topicCreated.ShouldBe(true);

        }

        private async Task<bool> FindTopicByName<T>()
        {
            var snsClient = CreateClientFactory().GetSnsClient(Region);
            var topic = await snsClient.FindTopicAsync(new DefaultNamingConventions().TopicName<T>()).ConfigureAwait(false);
            return topic != null;
        }
    }
}
