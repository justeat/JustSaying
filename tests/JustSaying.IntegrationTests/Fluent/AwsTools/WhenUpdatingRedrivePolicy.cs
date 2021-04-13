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
    public class WhenUpdatingRedrivePolicy : IntegrationTestBase
    {
        public WhenUpdatingRedrivePolicy(ITestOutputHelper outputHelper)
            : base(outputHelper)
        {
        }

        [AwsFact]
        public async Task Can_Update_Redrive_Policy()
        {
            // Arrange
            int maximumReceives = 42;

            ILoggerFactory loggerFactory = OutputHelper.ToLoggerFactory();
            IAwsClientFactory clientFactory = CreateClientFactory();

            var client = clientFactory.GetSqsClient(Region);

            var queue = new SqsQueueByName(
                Region,
                UniqueName,
                1,
                client,
                loggerFactory);

            await queue.CreateAsync(new SqsBasicConfiguration());

            // Act
            await queue.UpdateRedrivePolicyAsync(
                new RedrivePolicy(maximumReceives, queue.ErrorQueue.Arn));

            // Assert
            queue.RedrivePolicy.ShouldNotBeNull();
            queue.RedrivePolicy.MaximumReceives.ShouldBe(maximumReceives);
        }
    }
}
