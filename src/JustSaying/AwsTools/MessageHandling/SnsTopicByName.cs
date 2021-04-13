using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using JustSaying.AwsTools.QueueCreation;
using JustSaying.Messaging.Interrogation;
using JustSaying.Messaging.MessageSerialization;
using Microsoft.Extensions.Logging;

namespace JustSaying.AwsTools.MessageHandling
{
    [Obsolete("SnsTopicBase and related classes are not intended for general usage and may be removed in a future major release")]
    public class SnsTopicByName : SnsTopicBase
    {
        private readonly ILogger _logger;
        private bool _topicNameIsArn;

        public string TopicName { get; }
        public IDictionary<string, string> Tags { get; set; }

        public SnsTopicByName(
            string topicName,
            IAmazonSimpleNotificationService client,
            IMessageSerializationRegister serializationRegister,
            ILoggerFactory loggerFactory,
            IMessageSubjectProvider messageSubjectProvider)
            : base(serializationRegister, loggerFactory, messageSubjectProvider)
        {
            TopicName = topicName;
            Client = client;
            _logger = loggerFactory.CreateLogger("JustSaying");
        }

        public SnsTopicByName(
            string topicName,
            IAmazonSimpleNotificationService client,
            IMessageSerializationRegister serializationRegister,
            ILoggerFactory loggerFactory,
            SnsWriteConfiguration snsWriteConfiguration,
            IMessageSubjectProvider messageSubjectProvider,
            bool topicNameIsArn = false)
            : base(serializationRegister, loggerFactory, snsWriteConfiguration, messageSubjectProvider)
        {
            TopicName = topicName;
            _topicNameIsArn = topicNameIsArn;

            Client = client;
            _logger = loggerFactory.CreateLogger("JustSaying");
        }

        public override InterrogationResult Interrogate()
        {
            return new InterrogationResult(new
            {
                Arn,
                TopicName
            });
        }

        public async Task EnsurePolicyIsUpdatedAsync(IReadOnlyCollection<string> additionalSubscriberAccounts)
        {
            if (additionalSubscriberAccounts.Any())
            {
                var policyDetails = new SnsPolicyDetails
                {
                    AccountIds = additionalSubscriberAccounts,
                    SourceArn = Arn
                };
                await SnsPolicy.SaveAsync(policyDetails, Client).ConfigureAwait(false);
            }
        }

        public async Task ApplyTagsAsync()
        {
            if (!Tags.Any())
            {
                return;
            }

            Tag CreateTag(KeyValuePair<string, string> tag) => new() { Key = tag.Key, Value = tag.Value };

            var tagRequest = new TagResourceRequest
            {
                ResourceArn = Arn,
                Tags = Tags.Select(CreateTag).ToList()
            };

            await Client.TagResourceAsync(tagRequest).ConfigureAwait(false);

            _logger.LogInformation("Added {TagCount} tags to topic {TopicName}", tagRequest.Tags.Count, TopicName);
        }

        public override async Task<bool> ExistsAsync()
        {
            if (!string.IsNullOrWhiteSpace(Arn))
            {
                return true;
            }

            _logger.LogInformation("Checking for existence of the topic '{TopicName}'.", TopicName);
            if (_topicNameIsArn)
            {
                bool topicExists = await CheckTopicExistsByArn();
                if (topicExists)
                {
                    Arn = TopicName;
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                Topic topic = await CheckTopicExistsByName();

                if (topic != null)
                {
                    Arn = topic.TopicArn;
                    return true;
                }

                return false;
            }
        }

        private async Task<bool> CheckTopicExistsByArn()
        {
            bool exists = false;
            ListTopicsResponse response;
            do
            {
                response = await Client.ListTopicsAsync();
                exists = response.Topics.Any(topic => topic.TopicArn == TopicName);
            } while (!exists && response.NextToken != null);

            return exists;
        }

        private async Task<Topic> CheckTopicExistsByName()
        {
            var topic = await Client.FindTopicAsync(TopicName).ConfigureAwait(false);
            return topic;
        }

        public async Task CreateAsync()
        {
            try
            {
                var response = await Client.CreateTopicAsync(new CreateTopicRequest(TopicName))
                    .ConfigureAwait(false);

                if (string.IsNullOrEmpty(response.TopicArn))
                {
                    var requestId = response.ResponseMetadata.RequestId;
                    _logger.LogError("Failed to create or obtain ARN for topic {TopicName}. RequestId: {RequestId}.",
                        TopicName, requestId);
                    throw new InvalidOperationException($"Failed to create or obtain ARN for topic '{TopicName}'. RequestId: {requestId}.");
                }

                Arn = response.TopicArn;
                _logger.LogDebug("Created topic '{TopicName}' with ARN '{Arn}'.", TopicName, Arn);
            }
            catch (AuthorizationErrorException ex)
            {
                _logger.LogWarning(0, ex, "Not authorized to create topic '{TopicName}'.", TopicName);
                if (!await ExistsAsync().ConfigureAwait(false))
                {
                    throw new InvalidOperationException(
                        $"Topic '{TopicName}' does not exist, and no permission to create it.");
                }
            }
        }

        public async Task CreateWithEncryptionAsync(ServerSideEncryption config)
        {
            await CreateAsync().ConfigureAwait(false);

            ServerSideEncryption =
                await ExtractServerSideEncryptionFromTopicAttributes().ConfigureAwait(false);

            await EnsureServerSideEncryptionIsUpdatedAsync(config).ConfigureAwait(false);
        }

        private async Task<ServerSideEncryption> ExtractServerSideEncryptionFromTopicAttributes()
        {
            var attributesResponse = await Client.GetTopicAttributesAsync(Arn).ConfigureAwait(false);

            if (!attributesResponse.Attributes.TryGetValue(
                    JustSayingConstants.AttributeEncryptionKeyId,
                    out var encryptionKeyId))
            {
                return null;
            }

            return new ServerSideEncryption
            {
                KmsMasterKeyId = encryptionKeyId
            };
        }

        private async Task EnsureServerSideEncryptionIsUpdatedAsync(ServerSideEncryption config)
        {
            if (ServerSideEncryptionNeedsUpdating(config))
            {
                var request = new SetTopicAttributesRequest
                {
                    TopicArn = Arn,
                    AttributeName = JustSayingConstants.AttributeEncryptionKeyId,
                    AttributeValue = config?.KmsMasterKeyId ?? string.Empty
                };

                var response = await Client.SetTopicAttributesAsync(request).ConfigureAwait(false);

                if (response.HttpStatusCode == HttpStatusCode.OK)
                {
                    ServerSideEncryption = string.IsNullOrEmpty(config?.KmsMasterKeyId)
                        ? null
                        : config;
                }
                else
                {
                    _logger.LogWarning(
                        "Request to set topic attribute '{TopicAttributeName}' to '{TopicAttributeValue}' failed with status code '{HttpStatusCode}'.",
                        request.AttributeName,
                        request.AttributeValue,
                        response.HttpStatusCode);
                }
            }
        }

        private bool ServerSideEncryptionNeedsUpdating(ServerSideEncryption config)
        {
            if (ServerSideEncryption == config)
            {
                return false;
            }

            if (ServerSideEncryption != null && config != null)
            {
                return ServerSideEncryption.KmsMasterKeyId != config.KmsMasterKeyId;
            }

            return true;
        }
    }
}
