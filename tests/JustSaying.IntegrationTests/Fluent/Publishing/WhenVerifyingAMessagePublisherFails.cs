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
    public class WhenVerifyingAMessagePublisherFails : IntegrationTestBase
    {
        public WhenVerifyingAMessagePublisherFails(ITestOutputHelper outputHelper)
            : base(outputHelper)
        {
        }

        [AwsFact]
        public async Task Then_The_Bus_Fails_Fast()
        {
            //Arrange
            var topicName = new UniqueTopicNamingConvention().TopicName<SimpleMessage>();

            var serviceProvider = GivenJustSaying()
                .ConfigureJustSaying(
                    builder => builder.Publications(
                        options =>
                        {
                            options.WithQueue<SimpleMessage>(UniqueName);
                            options.WithTopic<SimpleMessage>(configure =>
                            {
                                configure.WithTopic(topicName);
                                //The topic should already exists and we just want to verify it
                                configure.WithInfastructure(InfrastructureAction.ValidateExists);
                            });
                        })
                )
                .BuildServiceProvider();

            // Act - Try to validate a non-existant topic
            var result = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                {
                    IMessagePublisher publisher = serviceProvider.GetRequiredService<IMessagePublisher>();
                    await publisher.StartAsync(CancellationToken.None);
                }
            );

            // Assert

            result.Message.ShouldBe($"The topic {topicName} does not exist and infrastructure was set to validate");

        }
    }
}
