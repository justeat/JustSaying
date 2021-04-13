using System.Threading.Tasks;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.IntegrationTests.TestHandlers;
using JustSaying.Messaging.MessageHandling;
using JustSaying.Models;
using JustSaying.TestingFramework;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Xunit.Abstractions;
#pragma warning disable 618

namespace JustSaying.IntegrationTests.Fluent.Subscribing
{
    public class WhenHandlingMultipleTopics : IntegrationTestBase
    {
        public WhenHandlingMultipleTopics(ITestOutputHelper outputHelper)
            : base(outputHelper)
        {
        }

        [NotSimulatorFact]
        public async Task Sqs_Policy_Is_Applied_With_Wildcard()
        {
            // Arrange
            var services = GivenJustSaying()
                .ConfigureJustSaying((builder) => builder.WithLoopbackTopic<TopicA>(UniqueName))
                .ConfigureJustSaying((builder) => builder.WithLoopbackTopic<TopicB>(UniqueName))
                .AddJustSayingHandler<TopicA, HandlerA>()
                .AddJustSayingHandler<TopicB, HandlerB>();

            await WhenAsync(
                services,
                async (publisher, listener, serviceProvider, cancellationToken) =>
                {
                    await listener.StartAsync(cancellationToken);

                    var clientFactory = serviceProvider.GetRequiredService<MessagingBusBuilder>().BuildClientFactory();
                    var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
                    var client = clientFactory.GetSqsClient(Region);

                    var queue = new SqsQueueByName(Region, UniqueName, 0, client, loggerFactory);

                    await Patiently.AssertThatAsync(OutputHelper, () => queue.ExistsAsync(), 60.Seconds());

                    dynamic policyJson = JObject.Parse(queue.Policy);

                    policyJson.Statement.Count.ShouldBe(1, $"Expecting 1 statement in Sqs policy but found {policyJson.Statement.Count}.");
                });
        }

        private class TopicA : Message
        {
        }

        private class TopicB : Message
        {
        }

        private sealed class HandlerA : IHandlerAsync<TopicA>
        {
            public Task<bool> Handle(TopicA message)
            {
                return Task.FromResult(true);
            }
        }

        private sealed class HandlerB : IHandlerAsync<TopicB>
        {
            public Task<bool> Handle(TopicB message)
            {
                return Task.FromResult(true);
            }
        }
    }
}
