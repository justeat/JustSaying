using JustSaying.AwsTools.QueueCreation;

namespace JustSaying
{
    /// <summary>
    /// A default namign strategy for JustSaying bus.
    /// Topic names are defaulted to message type name, lowercase (one topic per message type).
    /// Queue name is default to queue name.
    /// 
    /// Such configuration gives a queue for each IntoQueue configuration , and a queue is subcribed to multiple topics, where one topic per message.
    /// </summary>
    class DefaultNamingStrategy : INamingStrategy
    {
        public string GetPublishEndpoint(IPublishConfiguration publishConfig, string messageType)
        {
            return publishConfig.Topic;
        }

        public string GetSubscriptionEndpoint(SqsReadConfiguration subscriptionConfig, string messageType)
        {

            return string.IsNullOrWhiteSpace(subscriptionConfig.BaseQueueName) ? messageType.ToLower() : subscriptionConfig.BaseQueueName.ToLower();
        }
    }
}