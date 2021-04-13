using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using JustSaying.AwsTools.QueueCreation;
using JustSaying.Extensions;
using Microsoft.Extensions.Logging;

namespace JustSaying.AwsTools.MessageHandling
{
    [Obsolete("SqsQueueBase and related classes are not intended for general usage and may be removed in a future major release")]
    public abstract class SqsQueueByNameBase : SqsQueueBase
    {
        private readonly bool _queueNameIsArn;

        protected SqsQueueByNameBase(
            RegionEndpoint region,
            string queueName,
            IAmazonSQS client,
            ILoggerFactory loggerFactory)
            : base(region, client, loggerFactory)
        {
            _queueNameIsArn = Amazon.Arn.IsArn(queueName);
            QueueName = queueName;
        }

        public override async Task<bool> ExistsAsync()
        {
            if (string.IsNullOrWhiteSpace(QueueName))
            {
                return false;
            }

            return _queueNameIsArn ? await CheckExistsByArn(): await CheckExistsByName();
        }

        private async Task<bool> CheckExistsByArn()
        {
            bool exists = false;
            ListQueuesResponse listQueuesResponse = new ListQueuesResponse();
            do
            {
                listQueuesResponse = await Client.ListQueuesAsync(new ListQueuesRequest{MaxResults = 100, QueueNamePrefix = Amazon.Arn.Parse(QueueName).Resource, NextToken = listQueuesResponse.NextToken});

                //Hopefully we get one, but possible we have synomyms, partial matches etc
                foreach (var queueUrl in listQueuesResponse.QueueUrls)
                {
                    var getQueueAttributesResponse = await Client.GetQueueAttributesAsync(new GetQueueAttributesRequest { QueueUrl = queueUrl });
                    if (getQueueAttributesResponse.QueueARN == QueueName)
                    {
                        exists = true;
                        Arn = QueueName;
                        break;
                    }
                }
            } while (!exists && listQueuesResponse.NextToken != null);

            return exists;
        }

        private async Task<bool> CheckExistsByName()
        {
            GetQueueUrlResponse result;

            try
            {
                using (Logger.Time(LogLevel.Debug, "Checking if queue '{QueueName}' exists", QueueName))
                {
                    result = await Client.GetQueueUrlAsync(QueueName).ConfigureAwait(false);
                }
            }
            catch (QueueDoesNotExistException)
            {
                return false;
            }

            if (result?.QueueUrl == null)
            {
                return false;
            }

            Uri = new Uri(result.QueueUrl);

            await SetQueuePropertiesAsync().ConfigureAwait(false);
            return true;
        }

        private static readonly TimeSpan CreateRetryDelay = TimeSpan.FromMinutes(1);

        public virtual async Task<bool> CreateAsync(SqsBasicConfiguration queueConfig, int attempt = 0)
        {
            // If we're on a delete timeout, throw after 3 attempts.
            const int maxAttempts = 3;

            try
            {
                var queueResponse = await Client.CreateQueueAsync(QueueName).ConfigureAwait(false);

                if (!string.IsNullOrWhiteSpace(queueResponse?.QueueUrl))
                {
                    Uri = new Uri(queueResponse.QueueUrl);
                    await Client.SetQueueAttributesAsync(queueResponse.QueueUrl, GetCreateQueueAttributes(queueConfig)).ConfigureAwait(false);
                    await SetQueuePropertiesAsync().ConfigureAwait(false);

                    Logger.LogInformation("Created queue '{QueueName}' with ARN '{Arn}'.", QueueName, Arn);
                    return true;
                }
            }
            catch (AmazonSQSException ex)
            {
                if (ex.ErrorCode == "AWS.SimpleQueueService.QueueDeletedRecently")
                {
                    if (attempt >= (maxAttempts - 1))
                    {
                        Logger.LogError(
                            ex,
                            "Error trying to create queue '{QueueName}'. Maximum retries of {MaxAttempts} exceeded for delay {Delay}.",
                            QueueName, maxAttempts,
                            CreateRetryDelay);

                        throw;
                    }

                    // Ensure we wait for queue delete timeout to expire.
                    Logger.LogInformation(
                        "Waiting to create queue '{QueueName}' for {Delay}, due to AWS time restriction. Attempt number {AttemptCount} of {MaxAttempts}.",
                        QueueName,
                        CreateRetryDelay,
                        attempt + 1,
                        maxAttempts);

                    await Task.Delay(CreateRetryDelay).ConfigureAwait(false);
                    await CreateAsync(queueConfig, attempt + 1).ConfigureAwait(false);
                }
                else
                {
                    // Throw all errors which are not delete timeout related.
                    Logger.LogError(ex, "Error trying to create queue '{QueueName}'.", QueueName);
                    throw;
                }
            }

            Logger.LogWarning("Failed to create queue '{QueueName}'.", QueueName);
            return false;
        }

        protected abstract Dictionary<string, string> GetCreateQueueAttributes(SqsBasicConfiguration queueConfig);
    }
}
