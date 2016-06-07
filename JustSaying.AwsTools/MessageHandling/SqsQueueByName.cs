using System.Collections.Generic;
using System.Globalization;
using System.Net;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.SQS.Util;
using JustSaying.AwsTools.QueueCreation;

namespace JustSaying.AwsTools.MessageHandling
{
    public class SqsQueueByName : SqsQueueByNameBase
    {
        private readonly int _retryCountBeforeSendingToErrorQueue;

        public SqsQueueByName(RegionEndpoint region, string queueName, IAmazonSQS client, int retryCountBeforeSendingToErrorQueue)
            : base(region, queueName, client)
        {
            _retryCountBeforeSendingToErrorQueue = retryCountBeforeSendingToErrorQueue;
            ErrorQueue = new ErrorQueue(region, queueName, client);
        }

        public override bool Create(SqsBasicConfiguration queueConfig, int attempt = 0)
        {
            if (NeedErrorQueue(queueConfig) && !ErrorQueue.Exists())
            {
                ErrorQueue.Create(new SqsBasicConfiguration { ErrorQueueRetentionPeriodSeconds = queueConfig.ErrorQueueRetentionPeriodSeconds, ErrorQueueOptOut = true });
            }
            return base.Create(queueConfig, attempt);
        }

        private static bool NeedErrorQueue(SqsBasicConfiguration queueConfig)
        {
            return !queueConfig.ErrorQueueOptOut;
        }

        public override void Delete()
        {
            if(ErrorQueue != null)
                ErrorQueue.Delete();
            base.Delete();
        }

        public void UpdateRedrivePolicy(RedrivePolicy requestedRedrivePolicy)
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
                var response = Client.SetQueueAttributes(request);
                if (response.HttpStatusCode == HttpStatusCode.OK)
                {
                    RedrivePolicy = requestedRedrivePolicy;
                }
            }
        }

        public void EnsureQueueAndErrorQueueExistAndAllAttributesAreUpdated(SqsBasicConfiguration queueConfig)
        {
            if (!Exists())
                Create(queueConfig);
            else
            {
                UpdateQueueAttribute(queueConfig);
            }

            //Create an error queue for existing queues if they don't already have one
            if (ErrorQueue != null && NeedErrorQueue(queueConfig))
            {
                var errorQueueConfig = new SqsReadConfiguration(SubscriptionType.ToTopic)
                {
                    ErrorQueueRetentionPeriodSeconds = queueConfig.ErrorQueueRetentionPeriodSeconds,
                    ErrorQueueOptOut = true
                };
                if (!ErrorQueue.Exists())
                {

                    ErrorQueue.Create(errorQueueConfig);
                }
                else
                {
                    ErrorQueue.UpdateQueueAttribute(errorQueueConfig);
                }
                UpdateRedrivePolicy(new RedrivePolicy(queueConfig.RetryCountBeforeSendingToErrorQueue, ErrorQueue.Arn));
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

            return policy;
        }

        private bool RedrivePolicyNeedsUpdating(RedrivePolicy requestedRedrivePolicy)
        {
            return RedrivePolicy == null || RedrivePolicy.MaximumReceives != requestedRedrivePolicy.MaximumReceives;
        }
    }
}