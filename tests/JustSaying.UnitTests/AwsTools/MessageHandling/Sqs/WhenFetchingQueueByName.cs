using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using JustSaying.AwsTools.MessageHandling;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Shouldly;
using Xunit;
#pragma warning disable 618

namespace JustSaying.UnitTests.AwsTools.MessageHandling.Sqs
{
    public class WhenFetchingQueueByName
    {
        private readonly IAmazonSQS _client;
        private readonly ILoggerFactory _log;
        private const int RetryCount = 3;

        public WhenFetchingQueueByName()
        {
            _client = Substitute.For<IAmazonSQS>();

            _client.GetQueueUrlAsync(Arg.Any<string>())
                .Returns(x =>
                {
                    if (x.Arg<string>() == "some-queue-name")
                        return new GetQueueUrlResponse
                        {
                            QueueUrl = "https://testqueues.com/some-queue-name"
                        };
                    throw new QueueDoesNotExistException("some-queue-name not found");
                });
            _client.GetQueueAttributesAsync(Arg.Any<GetQueueAttributesRequest>())
                .Returns(new GetQueueAttributesResponse()
                {
                    Attributes = new Dictionary<string, string> { { "QueueArn", "something:some-queue-name" } }
                });
            _log = Substitute.For<ILoggerFactory>();
        }

        [Fact]
        public async Task IncorrectQueueNameDoNotMatch()
        {
            var sqsQueueByName = new SqsQueueByName(RegionEndpoint.EUWest1, "some-queue-name1", RetryCount, _client, _log);
            (await sqsQueueByName.ExistsAsync()).ShouldBeFalse();
        }

        [Fact]
        public async Task IncorrectPartialQueueNameDoNotMatch()
        {
            var sqsQueueByName = new SqsQueueByName(RegionEndpoint.EUWest1, "some-queue", RetryCount, _client, _log);
            (await sqsQueueByName.ExistsAsync()).ShouldBeFalse();
        }

        [Fact]
        public async Task CorrectQueueNameShouldMatch()
        {
            var sqsQueueByName = new SqsQueueByName(RegionEndpoint.EUWest1, "some-queue-name", RetryCount, _client, _log);
            (await sqsQueueByName.ExistsAsync()).ShouldBeTrue();
        }
    }
}
