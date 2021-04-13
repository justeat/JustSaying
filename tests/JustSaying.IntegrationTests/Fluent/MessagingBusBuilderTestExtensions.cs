using System;
using JustSaying.Fluent;
using JustSaying.Models;

namespace JustSaying.IntegrationTests.Fluent
{
    internal static class MessagingBusBuilderTestExtensions
    {
        public static MessagingBusBuilder WithLoopbackQueue<T>(this MessagingBusBuilder builder, string name,
            Action<QueueSubscriptionBuilder<T>> configure = null)
            where T : Message
        {
            return builder
                .Publications((options) => options.WithQueue<T>(name))
                .Subscriptions((options) => options.ForQueue<T>(subscriptionBuilder =>
                {
                    subscriptionBuilder.WithQueue(name);
                    configure?.Invoke(subscriptionBuilder);
                }));
        }

        public static MessagingBusBuilder WithLoopbackTopic<T>(this MessagingBusBuilder builder, string name,
            Action<TopicSubscriptionBuilder<T>> configure = null)
            where T : Message
        {
            return builder
                .Publications((options) => options.WithTopic<T>())
                .Subscriptions((options) => options.ForTopic<T>(subscriptionBuilder =>
                {
                    subscriptionBuilder.WithQueue(name);
                    configure?.Invoke(subscriptionBuilder);
                }));
        }
    }
}
