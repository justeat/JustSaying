using System;
using System.Linq;
using Amazon;
using JustSaying.AwsTools;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.AwsTools.QueueCreation;
using JustSaying.Extensions;
using JustSaying.Messaging;
using JustSaying.Messaging.MessageHandling;
using JustSaying.Messaging.MessageSerialisation;
using JustSaying.Messaging.Monitoring;
using JustSaying.Models;
using NLog;
using JustSaying.Messaging.Interrogation;

namespace JustSaying
{
    /// <summary>
    /// Fluently configure a JustSaying message bus.
    /// Intended usage:
    /// 1. Factory.JustSaying(); // Gimme a bus
    /// 2. WithMonitoring(instance) // Ensure you monitor the messaging status
    /// 3. Set subscribers - WithSqsTopicSubscriber() / WithSnsTopicSubscriber() etc // ToDo: Shouldn't be enforced in base! Is a JE concern.
    /// 3. Set Handlers - WithTopicMessageHandler()
    /// </summary>
    public class JustSayingFluently : ISubscriberIntoQueue, IHaveFulfilledSubscriptionRequirements, IHaveFulfilledPublishRequirements, IMayWantOptionalSettings, IMayWantARegionPicker, IAmJustInterrogating
    {
        private static readonly Logger Log = LogManager.GetLogger("JustSaying"); // ToDo: Dangerous!
        private readonly IVerifyAmazonQueues _amazonQueueCreator;
        private readonly IAwsClientFactoryProxy _awsClientFactoryProxy;
        protected readonly IAmJustSaying Bus;
        private SqsReadConfiguration _subscriptionConfig = new SqsReadConfiguration(SubscriptionType.ToTopic);
        private IMessageSerialisationFactory _serialisationFactory;
        private Func<INamingStrategy> _busNamingStrategyFunc;

        internal protected JustSayingFluently(IAmJustSaying bus, IVerifyAmazonQueues queueCreator, IAwsClientFactoryProxy awsClientFactoryProxy)
        {
            Bus = bus;
            _amazonQueueCreator = queueCreator;
            this._awsClientFactoryProxy = awsClientFactoryProxy;
        }

        private string GetMessageTypeName<T>()
        {
            return typeof(T).ToTopicName();
        }

        public virtual INamingStrategy GetNamingStrategy()
        {
            if (_busNamingStrategyFunc != null)
                return _busNamingStrategyFunc();
            return new DefaultNamingStrategy();
        }

        /// <summary>
        /// Register for publishing messages to SNS
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IHaveFulfilledPublishRequirements WithSnsMessagePublisher<T>() where T : Message
        {
            Log.Info("Adding SNS publisher");
            _subscriptionConfig.Topic = GetMessageTypeName<T>();
            var namingStrategy = GetNamingStrategy();

            Bus.SerialisationRegister.AddSerialiser<T>(_serialisationFactory.GetSerialiser<T>());

            var topicName = namingStrategy.GetTopicName(_subscriptionConfig.BaseTopicName, GetMessageTypeName<T>());
            foreach (var region in Bus.Config.Regions)
            {
                var eventPublisher = new SnsTopicByName(
                    topicName,
                    _awsClientFactoryProxy.GetAwsClientFactory().GetSnsClient(RegionEndpoint.GetBySystemName(region)),
                    Bus.SerialisationRegister);

                if (!eventPublisher.Exists())
                    eventPublisher.Create();

                Bus.AddMessagePublisher<T>(eventPublisher, region);
            }

            Log.Info(string.Format("Created SNS topic publisher - Topic: {0}", _subscriptionConfig.Topic));

            return this;
        }

        /// <summary>
        /// Register for publishing messages to SQS
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IHaveFulfilledPublishRequirements WithSqsMessagePublisher<T>(Action<SqsWriteConfiguration> configBuilder) where T : Message
        {
            Log.Info("Adding SQS publisher");

            var config = new SqsWriteConfiguration();
            configBuilder(config);

            var messageTypeName = typeof(T).ToTopicName();
            var queueName = GetNamingStrategy().GetQueueName(new SqsReadConfiguration(SubscriptionType.PointToPoint){BaseQueueName = config.QueueName}, messageTypeName);

            Bus.SerialisationRegister.AddSerialiser<T>(_serialisationFactory.GetSerialiser<T>());

            foreach (var region in Bus.Config.Regions)
            {
                var regionEndpoint = RegionEndpoint.GetBySystemName(region);
                var eventPublisher = new SqsPublisher(
                    regionEndpoint,
                    queueName,
                    _awsClientFactoryProxy.GetAwsClientFactory().GetSqsClient(regionEndpoint),
                    config.RetryCountBeforeSendingToErrorQueue,
                    Bus.SerialisationRegister);

                if (!eventPublisher.Exists())
                    eventPublisher.Create(config);

                Bus.AddMessagePublisher<T>(eventPublisher, region);
            }

            Log.Info(string.Format("Created SQS publisher - MessageName: {0}, QueueName: {1}", messageTypeName, queueName));

            return this;
        }

        /// <summary>
        /// I'm done setting up. Fire up listening on this baby...
        /// </summary>
        public void StartListening()
        {
            Bus.Start();
            Log.Info("Started listening for messages");
        }

        /// <summary>
        /// Gor graceful shutdown of all listening threads
        /// </summary>
        public void StopListening()
        {
            Bus.Stop();
            Log.Info("Stopped listening for messages");
        }

        /// <summary>
        /// Publish a message to the stack.
        /// </summary>
        /// <param name="message"></param>
        public virtual void Publish(Message message)
        {
            if (Bus == null)
                throw new InvalidOperationException("You must register for message publication before publishing a message");
            
            Bus.Publish(message);
        }

        /// <summary>
        /// States whether the stack is listening for messages (subscriptions are running)
        /// </summary>
        public bool Listening { get { return (Bus != null) && Bus.Listening; } }

        public IMayWantOptionalSettings WithSerialisationFactory(IMessageSerialisationFactory factory)
        {
            _serialisationFactory = factory;
            return this;
        }

        public IMayWantOptionalSettings WithMessageLockStoreOf(IMessageLock messageLock)
        {
            Bus.MessageLock = messageLock;
            return this;
        }

        public IFluentSubscription ConfigureSubscriptionWith(Action<SqsReadConfiguration> configBuilder)
        {
            configBuilder(_subscriptionConfig);
            return this;
        }

        public ISubscriberIntoQueue WithSqsTopicSubscriber(string topicName = null)
        {
            _subscriptionConfig = new SqsReadConfiguration(SubscriptionType.ToTopic)
            {
                BaseTopicName = (topicName ?? string.Empty).ToLower()
            };
            return this;
        }

        public ISubscriberIntoQueue WithSqsPointToPointSubscriber()
        {
            _subscriptionConfig = new SqsReadConfiguration(SubscriptionType.PointToPoint);
            return this;
        }

        #region Implementation of Queue Subscription

        public IFluentSubscription IntoQueue(string queuename)
        {
            _subscriptionConfig.BaseQueueName = queuename;
            return this;
        }

        public IHaveFulfilledSubscriptionRequirements WithMessageHandler<T>(IHandler<T> handler) where T : Message
        {
            return WithMessageHandler(new BlockingHandler<T>(handler));
        }

        /// <summary>
        /// Set message handlers for the given topic
        /// </summary>
        /// <typeparam name="T">Message type to be handled</typeparam>
        /// <param name="handler">Handler for the message type</param>
        /// <returns></returns>
        public IHaveFulfilledSubscriptionRequirements WithMessageHandler<T>(IHandlerAsync<T> handler) where T : Message
        {
            // TODO - Subscription listeners should be just added once per queue,
            // and not for each message handler
            var thing =  _subscriptionConfig.SubscriptionType == SubscriptionType.PointToPoint
                ? PointToPointHandler<T>()
                : TopicHandler<T>();
            
            Bus.SerialisationRegister.AddSerialiser<T>(_serialisationFactory.GetSerialiser<T>());
            foreach (var region in Bus.Config.Regions)
            {
                Bus.AddMessageHandler(region, _subscriptionConfig.QueueName, () => handler);
            }
            var messageTypeName = GetMessageTypeName<T>();
            Log.Info(string.Format("Added a message handler - MessageName: {0}, QueueName: {1}, HandlerName: {2}", messageTypeName, _subscriptionConfig.QueueName, handler.GetType().Name));

            return thing;
        }

        public IHaveFulfilledSubscriptionRequirements WithMessageHandler<T>(IHandlerResolver handlerResolver) where T : Message
        {
            var thing = _subscriptionConfig.SubscriptionType == SubscriptionType.PointToPoint
                ? PointToPointHandler<T>()
                : TopicHandler<T>();

            Bus.SerialisationRegister.AddSerialiser<T>(_serialisationFactory.GetSerialiser<T>());

            var proposedHandlers = handlerResolver.ResolveHandlers<T>().ToList();

            if (proposedHandlers.Count == 0)
            {
                throw new HandlerNotRegisteredWithContainerException(string.Format("IHandler<{0}> is not registered in the container.", typeof(T).Name));
            }
            if (proposedHandlers.Count > 1)
            {
                throw new NotSupportedException(string.Format("There are more than one registration for IHandler<{0}>. JustSaying currently does not support multiple registration for IHandler<T>.", typeof(T).Name));
            }

            foreach (var region in Bus.Config.Regions)
            {
                Bus.AddMessageHandler(region, _subscriptionConfig.QueueName,
                    () => handlerResolver.ResolveHandlers<T>().Single());
            }
            

            Log.Info(string.Format("Added a message handler - Topic: {0}, QueueName: {1}, HandlerName: IHandler<{2}>", _subscriptionConfig.Topic, _subscriptionConfig.QueueName, typeof(T)));

            return thing;
        }

        private IHaveFulfilledSubscriptionRequirements TopicHandler<T>() where T : Message
        {
            var messageTypeName = GetMessageTypeName<T>();
            ConfigureSqsSubscriptionViaTopic(messageTypeName);

            foreach (var region in Bus.Config.Regions)
            {
                var queue = _amazonQueueCreator.EnsureTopicExistsWithQueueSubscribed(region, Bus.SerialisationRegister, _subscriptionConfig);
                CreateSubscriptionListener<T>(region, queue);
                Log.Info(string.Format("Created SQS topic subscription - Topic: {0}, QueueName: {1}", _subscriptionConfig.Topic, _subscriptionConfig.QueueName));
            }
          
            return this;
        }

        private IHaveFulfilledSubscriptionRequirements PointToPointHandler<T>() where T : Message
        {
            var messageTypeName = GetMessageTypeName<T>();
            ConfigureSqsSubscription(messageTypeName);

            foreach (var region in Bus.Config.Regions)
            {
                var queue = _amazonQueueCreator.EnsureQueueExists(region, _subscriptionConfig);
                CreateSubscriptionListener<T>(region, queue);
                Log.Info(string.Format("Created SQS subscriber - MessageName: {0}, QueueName: {1}", messageTypeName, _subscriptionConfig.QueueName));
            }
           
            return this;
        }

        private void CreateSubscriptionListener<T>(string region, SqsQueueBase queue) where T : Message
        {
            var sqsSubscriptionListener = new SqsNotificationListener(queue, Bus.SerialisationRegister, Bus.Monitor, _subscriptionConfig.OnError, Bus.MessageLock);
            sqsSubscriptionListener.Subscribers.Add(new Subscriber(typeof(T)));
            Bus.AddNotificationSubscriber(region, sqsSubscriptionListener);

            if (_subscriptionConfig.MaxAllowedMessagesInFlight.HasValue)
            {
                sqsSubscriptionListener.WithMaximumConcurrentLimitOnMessagesInFlightOf(_subscriptionConfig.MaxAllowedMessagesInFlight.Value);
            }

            if (_subscriptionConfig.MessageProcessingStrategy != null)
            {
                sqsSubscriptionListener.WithMessageProcessingStrategy(_subscriptionConfig.MessageProcessingStrategy);
            }
        }

        private void ConfigureSqsSubscriptionViaTopic(string messageTypeName)
        {
            var namingStrategy = GetNamingStrategy();
            _subscriptionConfig.PublishEndpoint = namingStrategy.GetTopicName(_subscriptionConfig.BaseTopicName, messageTypeName);
            _subscriptionConfig.Topic = namingStrategy.GetTopicName(_subscriptionConfig.BaseTopicName, messageTypeName);
            _subscriptionConfig.QueueName = namingStrategy.GetQueueName(_subscriptionConfig, messageTypeName);
            _subscriptionConfig.Validate();
        }

        private void ConfigureSqsSubscription(string messageTypeName)
        {
            _subscriptionConfig.ValidateSqsConfiguration();
            _subscriptionConfig.QueueName = GetNamingStrategy().GetQueueName(_subscriptionConfig, messageTypeName);
        }

        #endregion

        #region Implementation of IFluentMonitoring

        /// <summary>
        /// Provide your own monitoring implementation
        /// </summary>
        /// <param name="messageMonitor">Monitoring class to be used</param>
        /// <returns></returns>
        public IMayWantOptionalSettings WithMonitoring(IMessageMonitor messageMonitor)
        {
            Bus.Monitor = messageMonitor;
            return this;
        }

        #endregion

        public IHaveFulfilledPublishRequirements ConfigurePublisherWith(Action<IPublishConfiguration> confBuilder)
        {
            confBuilder(Bus.Config);
            Bus.Config.Validate();

            return this;
        }

        public IMayWantARegionPicker WithFailoverRegion(string region)
        {
            Bus.Config.Regions.Add(region);
            return this;
        }

        public IMayWantARegionPicker WithFailoverRegions(params string[] regions)
        {
            foreach (var region in regions)
            {
                Bus.Config.Regions.Add(region);
            }
            Bus.Config.Validate();
            return this;
        }

        public IMayWantOptionalSettings WithActiveRegion(Func<string> getActiveRegion)
        {
            Bus.Config.GetActiveRegion = getActiveRegion;
            return this;
        }

        public IInterrogationResponse WhatDoIHave()
        {
            var iterrogationBus = Bus as IAmJustInterrogating;
            return iterrogationBus.WhatDoIHave();
        }

        public IMayWantOptionalSettings WithNamingStrategy(Func<INamingStrategy> busNamingStrategy)
        {
            this._busNamingStrategyFunc = busNamingStrategy;
            return this;
        }

        public IMayWantOptionalSettings WithAwsClientFactory(Func<IAwsClientFactory> awsClientFactory)
        {
            this._awsClientFactoryProxy.SetAwsClientFactory(awsClientFactory);
            return this;
        }
    }

    public interface IMayWantOptionalSettings : IMayWantMonitoring, IMayWantMessageLockStore, IMayWantCustomSerialisation, 
        IMayWantAFailoverRegion, IMayWantNamingStrategy, IMayWantAwsClientFactory { }

    public interface IMayWantAwsClientFactory
    {
        IMayWantOptionalSettings WithAwsClientFactory(Func<IAwsClientFactory> awsClientFactory);
    }

    public interface IMayWantNamingStrategy
    {
        IMayWantOptionalSettings WithNamingStrategy(Func<INamingStrategy> busNamingStrategy);
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
        IMayWantOptionalSettings WithMessageLockStoreOf(IMessageLock messageLock);
    }

    public interface IMayWantCustomSerialisation : IAmJustSayingFluently
    {
        IMayWantOptionalSettings WithSerialisationFactory(IMessageSerialisationFactory factory);
    }

    public interface IAmJustSayingFluently : IMessagePublisher
    {
        IHaveFulfilledPublishRequirements ConfigurePublisherWith(Action<IPublishConfiguration> confBuilder);
        IHaveFulfilledPublishRequirements WithSnsMessagePublisher<T>() where T : Message;
        IHaveFulfilledPublishRequirements WithSqsMessagePublisher<T>(Action<SqsWriteConfiguration> config) where T : Message;

        /// <summary>
        /// Adds subscriber to topic. 
        /// </summary>
        /// <param name="topicName">Topic name to subscribe to. If left empty,
        /// topic name will be message type name</param>
        /// <returns></returns>
        ISubscriberIntoQueue WithSqsTopicSubscriber(string topicName = null);
        ISubscriberIntoQueue WithSqsPointToPointSubscriber();
        void StartListening();
        void StopListening();
        bool Listening { get; }
    }

    public interface IFluentSubscription
    {
        IHaveFulfilledSubscriptionRequirements WithMessageHandler<T>(IHandler<T> handler) where T : Message;

        IHaveFulfilledSubscriptionRequirements WithMessageHandler<T>(IHandlerAsync<T> handler) where T : Message;

        IHaveFulfilledSubscriptionRequirements WithMessageHandler<T>(IHandlerResolver handlerResolver) where T : Message;
        IFluentSubscription ConfigureSubscriptionWith(Action<SqsReadConfiguration> config);
    }

    public interface IHaveFulfilledSubscriptionRequirements : IAmJustSayingFluently, IFluentSubscription
    {
        
    }

    public interface ISubscriberIntoQueue
    {
        IFluentSubscription IntoQueue(string queuename);
    }

    public interface IHaveFulfilledPublishRequirements : IAmJustSayingFluently
    {
    }
}