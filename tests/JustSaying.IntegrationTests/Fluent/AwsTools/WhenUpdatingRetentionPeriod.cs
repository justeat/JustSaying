using System;
using System.Threading.Tasks;
using JustSaying.AwsTools;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.AwsTools.QueueCreation;
using JustSaying.TestingFramework;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit.Abstractions;
#pragma warning disable 618

namespace JustSaying.IntegrationTests.Fluent.AwsTools
{
    public class WhenUpdatingRetentionPeriod : IntegrationTestBase
    {
        public WhenUpdatingRetentionPeriod(ITestOutputHelper outputHelper)
            : base(outputHelper)
        {
        }

        [AwsFact]
        public async Task Can_Update_Retention_Period()
        {
            // Arrange
            var oldRetentionPeriod = TimeSpan.FromSeconds(600);
            var newRetentionPeriod = TimeSpan.FromSeconds(700);

            ILoggerFactory loggerFactory = OutputHelper.ToLoggerFactory();
            IAwsClientFactory clientFactory = CreateClientFactory();

            var client = clientFactory.GetSqsClient(Region);

            var queue = new SqsQueueByName(
                Region,
                UniqueName,
                1,
                client,
                loggerFactory);

            await queue.CreateAsync(
                new SqsBasicConfiguration { MessageRetention = oldRetentionPeriod });

            // Act
            await queue.UpdateQueueAttributeAsync(
                new SqsBasicConfiguration { MessageRetention = newRetentionPeriod });

            // Assert
            queue.MessageRetentionPeriod.ShouldBe(newRetentionPeriod);
        }
    }
}
