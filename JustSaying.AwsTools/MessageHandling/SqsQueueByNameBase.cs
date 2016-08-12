using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using JustSaying.AwsTools.QueueCreation;
using NLog;

namespace JustSaying.AwsTools.MessageHandling
{
    public abstract class SqsQueueByNameBase : SqsQueueBase
    {
        private static readonly Logger Log = LogManager.GetLogger("JustSaying");

        protected SqsQueueByNameBase(RegionEndpoint region, string queueName, IAmazonSQS client)
            : base(region, client)
        {
            QueueName = queueName;
        }

        public override bool Exists()
        {
            var result = Client.ListQueues(new ListQueuesRequest{ QueueNamePrefix = QueueName });
            Log.Info("Checking if queue '{0}' exists", QueueName);
            Url = result.QueueUrls.SingleOrDefault(x => Matches(x, QueueName));

            if (Url != null)
            {
                SetQueueProperties();
                return true;
            }

            return false;
        }
        private static bool Matches(string queueUrl, string queueName)
        {
            return queueUrl.Substring(queueUrl.LastIndexOf("/", StringComparison.InvariantCulture) + 1)
                .Equals(queueName, StringComparison.InvariantCultureIgnoreCase);
        }

        public virtual bool Create(SqsBasicConfiguration queueConfig, int attempt = 0)
        {
            try
            {
                var result = Client.CreateQueue(new CreateQueueRequest{
                    QueueName = QueueName,
                    Attributes = GetCreateQueueAttributes(queueConfig)});

                if (!string.IsNullOrWhiteSpace(result.QueueUrl))
                {
                    Url = result.QueueUrl;
                    SetQueueProperties();

                    Log.Info("Created Queue: {0} on Arn: {1}", QueueName, Arn);
                    return true;
                }
            }
            catch (AmazonSQSException ex)
            {
                if (ex.ErrorCode == "AWS.SimpleQueueService.QueueDeletedRecently")
                {
                    // Ensure we wait for queue delete timeout to expire.
                    Log.Info("Waiting to create Queue due to AWS time restriction - Queue: {0}, AttemptCount: {1}", QueueName, attempt + 1);
                    Thread.Sleep(60000);
                    Create(queueConfig, attempt: attempt++);
                }
                else
                {
                    // Throw all errors which are not delete timeout related.
                    Log.Error(ex, $"Create Queue error: {QueueName}");
                    throw;
                }

                // If we're on a delete timeout, throw after 2 attempts.
                if (attempt >= 2)
                {
                    Log.Error(ex, $"Create Queue error, max retries exceeded for delay - Queue: {QueueName}");
                    throw;
                }
            }

            Log.Info("Failed to create Queue: {0}", QueueName);
            return false;
        }

        protected abstract Dictionary<string, string> GetCreateQueueAttributes(SqsBasicConfiguration queueConfig);
    }
}
