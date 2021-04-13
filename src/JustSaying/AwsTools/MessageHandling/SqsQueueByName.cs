using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.SQS.Util;
using JustSaying.AwsTools.QueueCreation;
using JustSaying.Extensions;
using Microsoft.Extensions.Logging;

namespace JustSaying.AwsTools.MessageHandling
{
    [Obsolete("SqsQueueBase and related classes are not intended for general usage and may be removed in a future major release")]
    public class SqsQueueByName : SqsQueueByNameBase
    {
        private readonly int _retryCountBeforeSendingToErrorQueue;

        public SqsQueueByName(
            RegionEndpoint region,
            string queueName,
            int retryCountBeforeSendingToErrorQueue,
            IAmazonSQS client,
            ILoggerFactory loggerFactory)
            : base(region, queueName, client, loggerFactory)
        {
            _retryCountBeforeSendingToErrorQueue = retryCountBeforeSendingToErrorQueue;
            ErrorQueue = new ErrorQueue(region, queueName, client, loggerFactory);
        }

        public override async Task<bool> CreateAsync(SqsBasicConfiguration queueConfig, int attempt = 0)
        {
            if (NeedErrorQueue(queueConfig))
            {
                var exists = await ErrorQueue.ExistsAsync().ConfigureAwait(false);
                if (!exists)
                {
                    using (Logger.Time("Creating error queue {QueueName}", ErrorQueue.QueueName))
                    {
                        await ErrorQueue.CreateAsync(new SqsBasicConfiguration
                        {
                            ErrorQueueRetentionPeriod = queueConfig.ErrorQueueRetentionPeriod,
                            ErrorQueueOptOut = true
                        }).ConfigureAwait(false);
                    }
                }
                else
                {
                    Logger.LogInformation("Error queue {QueueName} already exists, skipping", ErrorQueue.QueueName);
                }
            }

            using (Logger.Time("Creating queue {QueueName} attempt number {AttemptNumber}",
                queueConfig.QueueName,
                attempt))
            {
                return await base.CreateAsync(queueConfig, attempt).ConfigureAwait(false);
            }
        }

        private static bool NeedErrorQueue(SqsBasicConfiguration queueConfig)
        {
            return !queueConfig.ErrorQueueOptOut;
        }

        public override async Task DeleteAsync()
        {
            if (ErrorQueue != null)
            {
                await ErrorQueue.DeleteAsync().ConfigureAwait(false);
            }

            await base.DeleteAsync().ConfigureAwait(false);
        }

        public async Task UpdateRedrivePolicyAsync(RedrivePolicy requestedRedrivePolicy)
        {
            if (RedrivePolicyNeedsUpdating(requestedRedrivePolicy))
            {
                var request = new SetQueueAttributesRequest
                {
                    QueueUrl = Uri.AbsoluteUri,
                    Attributes = new Dictionary<string, string>
                        {
                            {JustSayingConstants.AttributeRedrivePolicy, requestedRedrivePolicy.ToString()}
                        }
                };

                var response = await Client.SetQueueAttributesAsync(request).ConfigureAwait(false);

                if (response?.HttpStatusCode == HttpStatusCode.OK)
                {
                    RedrivePolicy = requestedRedrivePolicy;
                }
            }
        }

        public async Task EnsureQueueAndErrorQueueExistAndAllAttributesAreUpdatedAsync(SqsReadConfiguration queueConfig)
        {
            if (queueConfig == null) throw new ArgumentNullException(nameof(queueConfig));

            var exists = await ExistsAsync().ConfigureAwait(false);
            if (!exists)
            {
                await CreateAsync(queueConfig).ConfigureAwait(false);
            }
            else
            {
                await UpdateQueueAttributeAsync(queueConfig).ConfigureAwait(false);
            }

            await ApplyTagsAsync(this, queueConfig.Tags).ConfigureAwait(false);

            //Create an error queue for existing queues if they don't already have one
            if (ErrorQueue != null && NeedErrorQueue(queueConfig))
            {
                var errorQueueConfig = new SqsReadConfiguration(SubscriptionType.ToTopic)
                {
                    ErrorQueueRetentionPeriod = queueConfig.ErrorQueueRetentionPeriod,
                    ErrorQueueOptOut = true
                };

                var errorQueueExists = await ErrorQueue.ExistsAsync().ConfigureAwait(false);
                if (!errorQueueExists)
                {
                    await ErrorQueue.CreateAsync(errorQueueConfig).ConfigureAwait(false);
                }
                else
                {
                    await ErrorQueue.UpdateQueueAttributeAsync(errorQueueConfig).ConfigureAwait(false);
                }

                await UpdateRedrivePolicyAsync(
                    new RedrivePolicy(queueConfig.RetryCountBeforeSendingToErrorQueue, ErrorQueue.Arn)).ConfigureAwait(false);

                await ApplyTagsAsync(ErrorQueue, queueConfig.Tags).ConfigureAwait(false);
            }
        }

        private async Task ApplyTagsAsync(ISqsQueue queue, Dictionary<string, string> tags)
        {
            if (tags is null || !tags.Any())
            {
                return;
            }

            var tagRequest = new TagQueueRequest
            {
                QueueUrl = queue.Uri.ToString(),
                Tags = tags
            };

            await queue.Client.TagQueueAsync(tagRequest).ConfigureAwait(false);

            Logger.LogInformation("Added {TagCount} tags to queue {QueueName}", tagRequest.Tags.Count, QueueName);
        }

        protected override Dictionary<string, string> GetCreateQueueAttributes(SqsBasicConfiguration queueConfig)
        {
            var policy = new Dictionary<string, string>
            {
                { SQSConstants.ATTRIBUTE_MESSAGE_RETENTION_PERIOD ,queueConfig.MessageRetention.AsSecondsString() },
                { SQSConstants.ATTRIBUTE_VISIBILITY_TIMEOUT  , queueConfig.VisibilityTimeout.AsSecondsString() },
                { SQSConstants.ATTRIBUTE_DELAY_SECONDS  , queueConfig.DeliveryDelay.AsSecondsString() },
            };

            if (NeedErrorQueue(queueConfig))
            {
                policy.Add(JustSayingConstants.AttributeRedrivePolicy, new RedrivePolicy(_retryCountBeforeSendingToErrorQueue, ErrorQueue.Arn).ToString());
            }

            if (queueConfig.ServerSideEncryption != null)
            {
                policy.Add(JustSayingConstants.AttributeEncryptionKeyId, queueConfig.ServerSideEncryption.KmsMasterKeyId);
                policy.Add(JustSayingConstants.AttributeEncryptionKeyReusePeriodSecondId, queueConfig.ServerSideEncryption.KmsDataKeyReusePeriodSeconds);
            }

            return policy;
        }

        private bool RedrivePolicyNeedsUpdating(RedrivePolicy requestedRedrivePolicy)
            => RedrivePolicy == null || RedrivePolicy.MaximumReceives != requestedRedrivePolicy.MaximumReceives;
    }
}
