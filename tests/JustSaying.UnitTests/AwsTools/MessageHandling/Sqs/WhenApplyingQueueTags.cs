using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.AwsTools.QueueCreation;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;
#pragma warning disable 618

namespace JustSaying.UnitTests.AwsTools.MessageHandling.Sqs
{
    public class WhenApplyingQueueTags
    {
        private const string QueueName = "my-queue-name";
        private const string QueueArn = "my-queue-arn";
        private const string QueueUrl = "http://my-queue-name/";
        private const string ErrorQueueName = "my-queue-name_error";
        private const string ErrorQueueArn = "my-queue-arn-error";
        private const string ErrorQueueUrl = "http://my-queue-name-error/";

        private readonly IAmazonSQS _client;

        public WhenApplyingQueueTags()
        {
            _client = Substitute.For<IAmazonSQS>();

            _client.GetQueueUrlAsync(Arg.Any<string>())
                .Returns(callInfo =>
                {
                    string queueUrl = callInfo.Arg<string>() switch
                    {
                        QueueName => QueueUrl,
                        ErrorQueueName => ErrorQueueUrl,
                        _ => throw new QueueDoesNotExistException("Not found")
                    };

                    return new GetQueueUrlResponse { QueueUrl = queueUrl };
                });

            _client.GetQueueAttributesAsync(Arg.Any<GetQueueAttributesRequest>())
                .Returns(callInfo =>
                {
                    string queueArn = callInfo.Arg<GetQueueAttributesRequest>().QueueUrl switch
                    {
                        QueueUrl => QueueArn,
                        ErrorQueueUrl => ErrorQueueArn,
                        _ => throw new QueueDoesNotExistException("Not found")
                    };

                    return new GetQueueAttributesResponse
                    {
                        Attributes = new Dictionary<string, string>
                        {
                            ["QueueArn"] = queueArn
                        }
                    };
                });

            _client.SetQueueAttributesAsync(Arg.Any<SetQueueAttributesRequest>()).Returns(new SetQueueAttributesResponse
            {
                HttpStatusCode = HttpStatusCode.OK
            });
        }

        [Fact]
        public async Task TagsAreAppliedToParentAndErrorQueues()
        {
            // Arrange
            var sut = new SqsQueueByName(RegionEndpoint.EUWest1, QueueName, 3, _client, NullLoggerFactory.Instance);

            var config = new SqsReadConfiguration(SubscriptionType.ToTopic)
            {
                Tags = new Dictionary<string, string>
                {
                    ["TagOne"] = "tag-one",
                    ["TagTwo"] = "tag-two"
                }
            };

            // Act
            await sut.EnsureQueueAndErrorQueueExistAndAllAttributesAreUpdatedAsync(config);

            // Assert
            await _client.Received(1).TagQueueAsync(Arg.Is<TagQueueRequest>(req => req.QueueUrl == QueueUrl && req.Tags == config.Tags));
        }

        [Fact]
        public async Task TagsAreNotAppliedIfNoneAreProvided()
        {
            // Arrange
            var sut = new SqsQueueByName(RegionEndpoint.EUWest1, QueueName, 3, _client, NullLoggerFactory.Instance);

            // Act
            await sut.EnsureQueueAndErrorQueueExistAndAllAttributesAreUpdatedAsync(new SqsReadConfiguration(SubscriptionType.ToTopic));

            // Assert
            await _client.Received(0).TagQueueAsync(Arg.Any<TagQueueRequest>());
        }
    }
}
