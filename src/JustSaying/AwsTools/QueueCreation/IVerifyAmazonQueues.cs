using JustSaying.AwsTools.MessageHandling;
using JustSaying.Fluent;
using JustSaying.Messaging.MessageSerialization;

namespace JustSaying.AwsTools.QueueCreation
{
    public interface IVerifyAmazonQueues
    {
        QueueWithAsyncStartup EnsureTopicExistsWithQueueSubscribed(
            string region,
            IMessageSerializationRegister serializationRegister,
            SqsReadConfiguration queueConfig,
            IMessageSubjectProvider messageSubjectProvider,
            InfrastructureAction infrastructureAction,
            bool hasQueueArnNotName,
            bool hasTopicArnNotName);

        QueueWithAsyncStartup EnsureQueueExists(string region, bool hasArnNotName, SqsReadConfiguration queueConfig);
    }
}
