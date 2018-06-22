using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Threading.Tasks;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.SQS.Util;
using JustSaying.AwsTools.QueueCreation;
using Microsoft.Extensions.Logging;

namespace JustSaying.AwsTools.MessageHandling
{
    public class SqsQueueByName : SqsQueueByNameBase
    {
        private readonly int _retryCountBeforeSendingToErrorQueue;

        public SqsQueueByName(RegionEndpoint region, string queueName, IAmazonSQS client, int retryCountBeforeSendingToErrorQueue, ILoggerFactory loggerFactory)
            : base(region, queueName, client, loggerFactory)
        {
            _retryCountBeforeSendingToErrorQueue = retryCountBeforeSendingToErrorQueue;
            ErrorQueue = new ErrorQueue(region, queueName, client, loggerFactory);
        }

        public override async Task<bool> CreateAsync(SqsBasicConfiguration queueConfig, int attempt = 0)
        {
            if (NeedErrorQueue(queueConfig))
            {
                var exisits = await ErrorQueue.ExistsAsync().ConfigureAwait(false);
                if (!exisits)
                {
                    await ErrorQueue.CreateAsync(
                        new SqsBasicConfiguration { ErrorQueueRetentionPeriodSeconds = queueConfig.ErrorQueueRetentionPeriodSeconds, ErrorQueueOptOut = true }).ConfigureAwait(false);
                }
            }

            return await base.CreateAsync(queueConfig, attempt).ConfigureAwait(false);
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
                    QueueUrl = Url,
                    Attributes = new Dictionary<string, string>
                        {
                            {JustSayingConstants.ATTRIBUTE_REDRIVE_POLICY, requestedRedrivePolicy.ToString()}
                        }
                };

                var response = await Client.SetQueueAttributesAsync(request).ConfigureAwait(false);

                if (response?.HttpStatusCode == HttpStatusCode.OK)
                {
                    RedrivePolicy = requestedRedrivePolicy;
                }
            }
        }

        public async Task EnsureQueueAndErrorQueueExistAndAllAttributesAreUpdatedAsync(SqsBasicConfiguration queueConfig)
        {
            var exists = await ExistsAsync().ConfigureAwait(false);
            if (!exists)
            {
                await CreateAsync(queueConfig).ConfigureAwait(false);
            }
            else
            {
                await UpdateQueueAttributeAsync(queueConfig).ConfigureAwait(false);
            }

            //Create an error queue for existing queues if they don't already have one
            if (ErrorQueue != null && NeedErrorQueue(queueConfig))
            {
                var errorQueueConfig = new SqsReadConfiguration(SubscriptionType.ToTopic)
                {
                    ErrorQueueRetentionPeriodSeconds = queueConfig.ErrorQueueRetentionPeriodSeconds,
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
            }
        }

        protected override Dictionary<string, string> GetCreateQueueAttributes(SqsBasicConfiguration queueConfig)
        {
            var policy = new Dictionary<string, string>
            {
                { SQSConstants.ATTRIBUTE_MESSAGE_RETENTION_PERIOD ,queueConfig.MessageRetentionSeconds.ToString(CultureInfo.InvariantCulture)},
                { SQSConstants.ATTRIBUTE_VISIBILITY_TIMEOUT  , queueConfig.VisibilityTimeoutSeconds.ToString(CultureInfo.InvariantCulture)},
                { SQSConstants.ATTRIBUTE_DELAY_SECONDS  , queueConfig.DeliveryDelaySeconds.ToString(CultureInfo.InvariantCulture)},
            };
            if (NeedErrorQueue(queueConfig))
            {
                policy.Add(JustSayingConstants.ATTRIBUTE_REDRIVE_POLICY, new RedrivePolicy(_retryCountBeforeSendingToErrorQueue, ErrorQueue.Arn).ToString());
            }

            if (queueConfig.ServerSideEncryption != null)
            {
                policy.Add(JustSayingConstants.ATTRIBUTE_ENCRYPTION_KEY_ID, queueConfig.ServerSideEncryption.KmsMasterKeyId);
                policy.Add(JustSayingConstants.ATTRIBUTE_ENCRYPTION_KEY_REUSE_PERIOD_SECOND_ID, queueConfig.ServerSideEncryption.KmsDataKeyReusePeriodSeconds);
            }

            return policy;
        }

        private bool RedrivePolicyNeedsUpdating(RedrivePolicy requestedRedrivePolicy)
            => RedrivePolicy == null || RedrivePolicy.MaximumReceives != requestedRedrivePolicy.MaximumReceives;
    }
}
