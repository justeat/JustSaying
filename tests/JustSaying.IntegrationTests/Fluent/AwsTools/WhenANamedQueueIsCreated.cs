using System.Threading.Tasks;
using JustSaying.AwsTools;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.AwsTools.QueueCreation;
using JustSaying.TestingFramework;
using Microsoft.Extensions.Logging;
using Xunit.Abstractions;
#pragma warning disable 618

namespace JustSaying.IntegrationTests.Fluent.AwsTools
{
    public class WhenANamedQueueIsCreated : IntegrationTestBase
    {
        public WhenANamedQueueIsCreated(ITestOutputHelper outputHelper)
            : base(outputHelper)
        {
        }

        [AwsFact]
        public async Task Then_The_Error_Queue_Is_Created()
        {
            // Arrange
            ILoggerFactory loggerFactory = OutputHelper.ToLoggerFactory();
            IAwsClientFactory clientFactory = CreateClientFactory();

            var client = clientFactory.GetSqsClient(Region);

            var queue = new SqsQueueByName(
                Region,
                UniqueName,
                1,
                client,
                loggerFactory);

            // Act
            await queue.CreateAsync(new SqsBasicConfiguration());

            // Assert
            await Patiently.AssertThatAsync(OutputHelper,
                async () => await queue.ErrorQueue.ExistsAsync(),
                40.Seconds());
        }
    }
}
