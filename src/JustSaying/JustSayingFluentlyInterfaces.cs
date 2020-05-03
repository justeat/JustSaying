using System;
using System.Threading;
using JustSaying.AwsTools;
using JustSaying.AwsTools.QueueCreation;
using JustSaying.Messaging;
using JustSaying.Messaging.MessageHandling;
using JustSaying.Messaging.MessageSerialization;
using JustSaying.Messaging.Monitoring;

namespace JustSaying
{

    public interface IMayWantOptionalSettings : IMayWantMonitoring,
        IMayWantMessageLockStore,
        IMayWantCustomSerialization,
        IMayWantAFailoverRegion,
        IMayWantAwsClientFactory,
        IMayWantMessageContextAccessor
    {
    }

    public interface IMayWantAwsClientFactory
    {
        IMayWantOptionalSettings WithAwsClientFactory(Func<IAwsClientFactory> awsClientFactory);
    }

    public interface IMayWantAFailoverRegion
    {
        IMayWantARegionPicker WithFailoverRegion(string region);
    }

    public interface IMayWantARegionPicker : IMayWantAFailoverRegion
    {
        IMayWantOptionalSettings WithActiveRegion(Func<string> getActiveRegion);
    }

    public interface IMayWantMonitoring : IAmJustSayingFluently
    {
        IMayWantOptionalSettings WithMonitoring(IMessageMonitor messageMonitor);
    }

    public interface IMayWantMessageLockStore : IAmJustSayingFluently
    {
        IMayWantOptionalSettings WithMessageLockStoreOf(IMessageLockAsync messageLock);
    }

    public interface IMayWantCustomSerialization : IAmJustSayingFluently
    {
        IMayWantOptionalSettings WithSerializationFactory(IMessageSerializationFactory factory);
    }

    public interface IMayWantMessageContextAccessor : IAmJustSayingFluently
    {
        IMayWantOptionalSettings WithMessageContextAccessor(IMessageContextAccessor messageContextAccessor);
    }

    public interface IAmJustSayingFluently : IMessagePublisher
    {
        IHaveFulfilledPublishRequirements ConfigurePublisherWith(Action<IPublishConfiguration> confBuilder);
        IHaveFulfilledPublishRequirements WithSnsMessagePublisher<T>() where T : class;
        IHaveFulfilledPublishRequirements WithSnsMessagePublisher<T>(Action<SnsWriteConfiguration> config) where T : class;
        IHaveFulfilledPublishRequirements WithSqsMessagePublisher<T>(Action<SqsWriteConfiguration> config) where T : class;

        /// <summary>
        /// Adds subscriber to topic.
        /// </summary>
        /// <param name="topicName">Topic name to subscribe to. If left empty,
        /// topic name will be message type name</param>
        /// <returns></returns>
        ISubscriberIntoQueue WithSqsTopicSubscriber(string topicName = null);

        ISubscriberIntoQueue WithSqsPointToPointSubscriber();

        void StartListening(CancellationToken cancellationToken = default);
    }

    public interface IFluentSubscription
    {
        IHaveFulfilledSubscriptionRequirements WithMessageHandler<T>(IHandlerResolver handlerResolver) where T : class;

        IFluentSubscription ConfigureSubscriptionWith(Action<SqsReadConfiguration> config);
    }

    public interface IHaveFulfilledSubscriptionRequirements : IAmJustSayingFluently, IFluentSubscription
    {
    }

    public interface ISubscriberIntoQueue
    {
        IFluentSubscription IntoQueue(string queueName);
    }

    public interface IHaveFulfilledPublishRequirements : IAmJustSayingFluently
    {
    }
}
