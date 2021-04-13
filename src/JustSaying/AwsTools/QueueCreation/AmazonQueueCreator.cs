using System;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SQS;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.Fluent;
using JustSaying.Messaging.MessageSerialization;
using Microsoft.Extensions.Logging;

namespace JustSaying.AwsTools.QueueCreation
{
    public class AmazonQueueCreator : IVerifyAmazonQueues
    {
        private readonly IAwsClientFactoryProxy _awsClientFactory;
        private readonly ILoggerFactory _loggerFactory;

        private const string EmptyFilterPolicy = "{}";

        public AmazonQueueCreator(IAwsClientFactoryProxy awsClientFactory, ILoggerFactory loggerFactory)
        {
            _awsClientFactory = awsClientFactory;
            _loggerFactory = loggerFactory;
        }

        public QueueWithAsyncStartup EnsureTopicExistsWithQueueSubscribed(
            string region,
            IMessageSerializationRegister serializationRegister,
            SqsReadConfiguration queueConfig,
            IMessageSubjectProvider messageSubjectProvider,
            InfrastructureAction infrastructureAction = InfrastructureAction.CreateIfMissing,
            bool hasQueueArnNotName = false,
            bool hasTopicArnNotName = false)
        {
            //TODO: We may want to support provided Topic ARN but subscriber creates queue, which means we would need two InfrastrcutureAction variables
            if ((hasQueueArnNotName) && infrastructureAction == InfrastructureAction.CreateIfMissing)
                throw new InvalidOperationException($"With a Queue ARN the only supported action is validate");

            if ((hasTopicArnNotName) && infrastructureAction == InfrastructureAction.CreateIfMissing)
                throw new InvalidOperationException($"With a Queue ARN the only supported action is validate");

            var regionEndpoint = RegionEndpoint.GetBySystemName(region);
            var sqsClient = _awsClientFactory.GetAwsClientFactory().GetSqsClient(regionEndpoint);
            var snsClient = _awsClientFactory.GetAwsClientFactory().GetSnsClient(regionEndpoint);

            var queueWithStartup = EnsureQueueExists(region, hasQueueArnNotName, queueConfig);

            async Task StartupTask()
            {
                await queueWithStartup.StartupTask.Invoke().ConfigureAwait(false);
                var queue = queueWithStartup.Queue;
                if (infrastructureAction == InfrastructureAction.CreateIfMissing)
                {
                    if (TopicExistsInAnotherAccount(queueConfig))
                        await SubscribeToTopic(queueConfig, regionEndpoint, snsClient, sqsClient, queue);
                    else
                        await CreateAndSubscribeToTopic(serializationRegister, queueConfig, messageSubjectProvider, snsClient, sqsClient, queue);
                }
                else if (infrastructureAction == InfrastructureAction.ValidateExists)
                {
                    if (hasTopicArnNotName || TopicExistsInAnotherAccount(queueConfig))
                        await CheckTopicAndSubscriptionExistsByArn(queueConfig, regionEndpoint, snsClient, queue, hasTopicArnNotName);
                    else
                        await CheckTopicAndSubscriptioon(serializationRegister, queueConfig, messageSubjectProvider, snsClient, queue);
                }
            }

            return new QueueWithAsyncStartup(StartupTask, queueWithStartup.Queue);
        }

        private async Task CheckTopicAndSubscriptionExistsByArn(
            SqsReadConfiguration queueConfig,
            RegionEndpoint regionEndpoint,
            IAmazonSimpleNotificationService snsClient,
            ISqsQueue queue,
            bool hasTopicArnNotName)
        {
            ForeignTopicArnProvider arnProvider = hasTopicArnNotName ?
                new ForeignTopicArnProvider(queueConfig.TopicName, snsClient)
                :
                new ForeignTopicArnProvider(regionEndpoint, queueConfig.TopicSourceAccount, queueConfig.PublishEndpoint, snsClient);

            var exists = await arnProvider.ArnExistsAsync();
            if (!exists)
                throw new InvalidOperationException($"The topic {queueConfig.PublishEndpoint} in account {queueConfig.TopicSourceAccount} does not exist");

            await CheckSubscription(snsClient, queueConfig.TopicName, queue.Arn);
        }


       private async Task CheckTopicAndSubscriptioon(IMessageSerializationRegister serializationRegister, SqsReadConfiguration queueConfig, IMessageSubjectProvider messageSubjectProvider, IAmazonSimpleNotificationService snsClient, ISqsQueue queue)
       {
#pragma warning disable 618
                var eventTopic = new SnsTopicByName(queueConfig.PublishEndpoint,
                snsClient,
                serializationRegister,
                _loggerFactory,
                messageSubjectProvider);
#pragma warning restore 618

           var topicExists = await eventTopic.ExistsAsync();
           if (!topicExists)
               throw new InvalidOperationException($"The topic {queueConfig.PublishEndpoint} does not exist");

           await CheckSubscription(snsClient, eventTopic.Arn,queue.Arn);
       }

       private static async Task CheckSubscription(IAmazonSimpleNotificationService snsClient, string topicArn, string queueArn)
       {
           bool subExists = false;
           ListSubscriptionsByTopicResponse response;
           do
           {
               response = await snsClient.ListSubscriptionsByTopicAsync(new ListSubscriptionsByTopicRequest { TopicArn = topicArn });
               subExists = response.Subscriptions.Any(sub => (sub.Protocol.ToLower() == "sqs") && (sub.Endpoint == queueArn));
           } while (!subExists && response.NextToken != null);

           if (!subExists)
               throw new InvalidOperationException($"Could not find subscription on topic: {topicArn} for queue: {queueArn} ");
       }

       private static async Task SubscribeToTopic(SqsReadConfiguration queueConfig, RegionEndpoint regionEndpoint, IAmazonSimpleNotificationService snsClient, IAmazonSQS sqsClient, ISqsQueue queue)
        {
            var arnProvider = new ForeignTopicArnProvider(regionEndpoint,
                queueConfig.TopicSourceAccount,
                queueConfig.PublishEndpoint,
                snsClient);

            var topicArn = await arnProvider.GetArnAsync().ConfigureAwait(false);
            await SubscribeQueueAndApplyFilterPolicyAsync(snsClient,
                topicArn,
                sqsClient,
                queue.Uri,
                queueConfig.FilterPolicy).ConfigureAwait(false);
        }

        private async Task CreateAndSubscribeToTopic(IMessageSerializationRegister serializationRegister, SqsReadConfiguration queueConfig, IMessageSubjectProvider messageSubjectProvider, IAmazonSimpleNotificationService snsClient, IAmazonSQS sqsClient, ISqsQueue queue)
        {
#pragma warning disable 618
            var eventTopic = new SnsTopicByName(queueConfig.PublishEndpoint,
                snsClient,
                serializationRegister,
                _loggerFactory,
                messageSubjectProvider);
#pragma warning restore 618
            await eventTopic.CreateAsync().ConfigureAwait(false);

            await SubscribeQueueAndApplyFilterPolicyAsync(snsClient,
                eventTopic.Arn,
                sqsClient,
                queue.Uri,
                queueConfig.FilterPolicy).ConfigureAwait(false);

            var sqsDetails = new SqsPolicyDetails
            {
                SourceArn = eventTopic.Arn,
                QueueArn = queue.Arn,
                QueueUri = queue.Uri
            };
            await SqsPolicy
                .SaveAsync(sqsDetails, sqsClient)
                .ConfigureAwait(false);
        }


        private static bool TopicExistsInAnotherAccount(SqsReadConfiguration queueConfig)
        {
            return !string.IsNullOrWhiteSpace(queueConfig.TopicSourceAccount);
        }

        public QueueWithAsyncStartup EnsureQueueExists(
            string region,
            bool hasArnNotName,
            SqsReadConfiguration queueConfig)
        {
            var regionEndpoint = RegionEndpoint.GetBySystemName(region);
            var sqsClient = _awsClientFactory.GetAwsClientFactory().GetSqsClient(regionEndpoint);

#pragma warning disable 618
            var queue = new SqsQueueByName(regionEndpoint,
                queueConfig.QueueName,
                queueConfig.RetryCountBeforeSendingToErrorQueue,
                sqsClient,
                _loggerFactory);
#pragma warning restore 618

            Func<Task> startupTask;
            if (!hasArnNotName)
            {
                startupTask = new Func<Task>(() => queue.EnsureQueueAndErrorQueueExistAndAllAttributesAreUpdatedAsync(queueConfig));
            }
            else
            {
                startupTask = new Func<Task>(() => queue.ExistsAsync());
            }

            return new QueueWithAsyncStartup(startupTask, queue);
        }

        private static async Task SubscribeQueueAndApplyFilterPolicyAsync(
            IAmazonSimpleNotificationService amazonSimpleNotificationService,
            string topicArn,
            IAmazonSQS amazonSQS,
            Uri queueUrl,
            string filterPolicy)
        {
            if (amazonSimpleNotificationService == null) throw new ArgumentNullException(nameof(amazonSimpleNotificationService));
            if (amazonSQS == null) throw new ArgumentNullException(nameof(amazonSQS));
            if (queueUrl == null) throw new ArgumentNullException(nameof(queueUrl));
            if (string.IsNullOrEmpty(topicArn)) throw new ArgumentException($"{nameof(topicArn)} cannot be null or empty.", nameof(topicArn));

            var subscriptionArn = await amazonSimpleNotificationService
                .SubscribeQueueAsync(topicArn, amazonSQS, queueUrl.AbsoluteUri)
                .ConfigureAwait(false);

            var actualFilterPolicy =
                string.IsNullOrWhiteSpace(filterPolicy) ? EmptyFilterPolicy : filterPolicy;
            await amazonSimpleNotificationService
                .SetSubscriptionAttributesAsync(subscriptionArn, "FilterPolicy", actualFilterPolicy)
                .ConfigureAwait(false);
        }
    }
}
