using System;
using System.Collections.Generic;
using JustSaying.AwsTools.QueueCreation;
using JustSaying.Messaging.Middleware;
using JustSaying.Models;
using JustSaying.Naming;
using Microsoft.Extensions.Logging;

namespace JustSaying.Fluent
{
    /// <summary>
    /// A class representing a builder for a topic subscription. This class cannot be inherited.
    /// </summary>
    /// <typeparam name="T">
    /// The type of the message.
    /// </typeparam>
    public sealed class TopicSubscriptionBuilder<T> : ISubscriptionBuilder<T>
        where T : Message
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TopicSubscriptionBuilder{T}"/> class.
        /// </summary>
        internal TopicSubscriptionBuilder()
        { }

        /// <summary>
        /// Gets of sets the name of the queue
        /// </summary>
        private string Queue { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the topic name or Arn.
        /// </summary>
        private string Topic { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets a delegate to a method to use to configure SNS reads.
        /// </summary>
        private Action<SqsReadConfiguration> ConfigureReads { get; set; }

        /// <summary>
        /// Is the queue named via a naming strategy via the subscription data type (true) or passed in (false)
        /// </summary>
        private bool HasDefaultQueueName { get; set; } = true;

        /// <summary>
        /// Do we supply an ARN and not use the name at all?
        /// </summary>
        public bool HasQueueArnNotName { get; set; }

        /// <summary>
        /// Just use the naming convention to create the topic name from type name
        /// </summary>
        private bool HasDefaultTopicName { get; set; } = true;

        /// <summary>
        /// Do we supply an ARN and not use the name at all?
        /// </summary>
        private bool HasTopicArnNotName { get; set; }

        /// <summary>
        /// What should we do about required infrastructure
        /// </summary>
        private InfrastructureAction InfrastructureAction { get; set; } = InfrastructureAction.CreateIfMissing;

        /// <summary>
        /// Gets the tags to add to the queue.
        /// </summary>
        private Dictionary<string, string> Tags { get; } = new(StringComparer.Ordinal);

        /// <summary>
        /// Configures that the queue name will be set to that of the topic
        /// </summary>
        /// <returns>The current <see cref="TopicSubscriptionBuilder{T}"/>> </returns>
        public TopicSubscriptionBuilder<T> UsingDefaultQueue() => WithQueue();


        /// <summary>
        /// Configures that the <see cref="ITopicNamingConvention"/> will create the topic topicName that should be used.
        /// </summary>
        /// <returns>
        /// The current <see cref="TopicSubscriptionBuilder{T}"/>.
        /// </returns>
        public TopicSubscriptionBuilder<T> IntoDefaultTopic() => WithTopic();

        public TopicSubscriptionBuilder<T> WithQueue(string queueName = null, string queueArn = null)
        {
            bool emptyName = string.IsNullOrEmpty(queueName);
            bool emptyArn = string.IsNullOrEmpty(queueArn);

            if (!emptyArn && !emptyName)
            {
                HasDefaultQueueName = true;
                HasQueueArnNotName = false;
            }

            //if we supply both, just use the Arn
            if (!emptyArn)
            {
                HasQueueArnNotName = true;
                HasDefaultQueueName = false;
                Queue = queueArn;
            }

            if (!emptyName)
            {
                HasDefaultQueueName = false;
                HasQueueArnNotName = false;
                Queue = queueName;
            }

            return this;

        }

       /// <summary>
        /// We need a topic to send a message to. Do we want to create it, or do we want to validate it exists?
        /// </summary>
        /// <param name="action">The action to take with respect to topics</param>
        /// <returns></returns>
        public TopicSubscriptionBuilder<T> WithInfrastructure(InfrastructureAction action)
        {
            InfrastructureAction = action;
            return this;
        }

        /// <summary>
        /// Overrides the policy of generating the topic from the type name, and allows it to be passed in instead
        /// </summary>
        /// <param name="topicName">The topic name to use</param>
        /// <param name="topicARN">We do not supply a name, instead use this externally created Arn instead. Should be used with validate infrastructure only</param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public TopicSubscriptionBuilder<T> WithTopic(string topicName = null, string topicARN = null)
        {
            bool emptyName = string.IsNullOrEmpty(topicName);
            bool emptyArn = string.IsNullOrEmpty(topicARN);

            if (!emptyArn && !emptyName)
            {
                HasDefaultTopicName = true;
                HasTopicArnNotName = false;
            }

            //if we supply both, just use the Arn
            if (!emptyArn)
            {
                HasTopicArnNotName = true;
                HasDefaultTopicName = false;
                Topic = topicARN;
            }

            if (!emptyName)
            {
                HasDefaultTopicName = false;
                HasTopicArnNotName = false;
                Topic = topicName;
            }

            return this;

        }

        /// <summary>
        /// Configures the SNS read configuration.
        /// </summary>
        /// <param name="configure">A delegate to a method to use to configure SNS reads.</param>
        /// <returns>
        /// The current <see cref="TopicSubscriptionBuilder{T}"/>.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="configure"/> is <see langword="null"/>.
        /// </exception>
        public TopicSubscriptionBuilder<T> WithReadConfiguration(
            Action<SqsReadConfigurationBuilder> configure)
        {
            if (configure == null)
            {
                throw new ArgumentNullException(nameof(configure));
            }

            var builder = new SqsReadConfigurationBuilder();

            configure(builder);

            ConfigureReads = builder.Configure;
            return this;
        }

        /// <summary>
        /// Configures the SNS read configuration.
        /// </summary>
        /// <param name="configure">A delegate to a method to use to configure SNS reads.</param>
        /// <returns>
        /// The current <see cref="TopicSubscriptionBuilder{T}"/>.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="configure"/> is <see langword="null"/>.
        /// </exception>
        public TopicSubscriptionBuilder<T> WithReadConfiguration(Action<SqsReadConfiguration> configure)
        {
            ConfigureReads = configure ?? throw new ArgumentNullException(nameof(configure));
            return this;
        }

        /// <summary>
        /// Creates a tag with no value that will be assigned to the SQS queue.
        /// </summary>
        /// <param name="key">The key for the tag.</param>
        /// <returns>
        /// The current <see cref="TopicSubscriptionBuilder{T}"/>.
        /// </returns>
        /// <remarks>Tag keys are case-sensitive. A new tag with a key identical to that of an existing one will overwrite it.</remarks>
        /// <exception cref="ArgumentException">
        /// <paramref name="key"/> is <see langword="null"/> or whitespace.
        /// </exception>
        public TopicSubscriptionBuilder<T> WithTag(string key) => WithTag(key, null);

        /// <summary>
        /// Creates a tag with a value that will be assigned to the SQS queue.
        /// </summary>
        /// <param name="key">The key for the tag.</param>
        /// <param name="value">The value associated with this tag.</param>
        /// <returns>
        /// The current <see cref="TopicSubscriptionBuilder{T}"/>.
        /// </returns>
        /// <remarks>Tag keys are case-sensitive. A new tag with a key identical to that of an existing one will overwrite it.</remarks>
        /// <exception cref="ArgumentException">
        /// <paramref name="key"/> is <see langword="null"/> or whitespace.
        /// </exception>
        public TopicSubscriptionBuilder<T> WithTag(string key, string value)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentException("A queue tag key cannot be null or only whitespace", nameof(key));
            }

            Tags.Add(key, value ?? string.Empty);

            return this;
        }

        /// <inheritdoc />
        void ISubscriptionBuilder<T>.Configure(
            JustSayingBus bus,
            IHandlerResolver handlerResolver,
            IServiceResolver serviceResolver,
            IVerifyAmazonQueues creator,
            ILoggerFactory loggerFactory)
        {
            var logger = loggerFactory.CreateLogger<TopicSubscriptionBuilder<T>>();

            var subscriptionConfig = new SqsReadConfiguration(SubscriptionType.ToTopic)
            {
                Tags = Tags
            };

            var config = bus.Config;

            ConfigureReads?.Invoke(subscriptionConfig);

            if (HasDefaultTopicName)
            {
                subscriptionConfig.ApplyTopicNamingConvention<T>(config.TopicNamingConvention);
            }
            else
            {
                subscriptionConfig.TopicName = Topic;
            }

            if (HasDefaultQueueName)
            {
                subscriptionConfig.ApplyQueueNamingConvention<T>(config.QueueNamingConvention);
            }
            else
            {
                subscriptionConfig.QueueName = Queue;
            }

            subscriptionConfig.SubscriptionGroupName ??= subscriptionConfig.QueueName;
            subscriptionConfig.PublishEndpoint = subscriptionConfig.TopicName;
            subscriptionConfig.MiddlewareConfiguration = subscriptionConfig.MiddlewareConfiguration;
            subscriptionConfig.Validate();

            var queueWithStartup = creator.EnsureTopicExistsWithQueueSubscribed(
                config.Region,
                bus.SerializationRegister,
                subscriptionConfig,
                config.MessageSubjectProvider,
                InfrastructureAction,
                HasQueueArnNotName,
                HasTopicArnNotName);

            bus.AddStartupTask(queueWithStartup.StartupTask);
            bus.AddQueue(subscriptionConfig.SubscriptionGroupName, queueWithStartup.Queue);

            logger.LogInformation(
                "Created SQS topic subscription on topic '{Topic}' and queue '{QueueName}'.",
                subscriptionConfig.TopicName,
                subscriptionConfig.QueueName);

            var resolutionContext = new HandlerResolutionContext(subscriptionConfig.QueueName);
            var proposedHandler = handlerResolver.ResolveHandler<T>(resolutionContext);
            if (proposedHandler == null)
            {
                throw new HandlerNotRegisteredWithContainerException(
                    $"There is no handler for '{typeof(T)}' messages.");
            }

            var middlewareBuilder = new HandlerMiddlewareBuilder(handlerResolver, serviceResolver);

            var handlerMiddleware = middlewareBuilder
                .UseHandler<T>()
                .UseStopwatch(proposedHandler.GetType())
                .Configure(subscriptionConfig.MiddlewareConfiguration)
                .Build();

            bus.AddMessageMiddleware<T>(subscriptionConfig.QueueName, handlerMiddleware);

            logger.LogInformation(
                "Added a message handler for message type for '{MessageType}' on topic '{Topic}' and queue '{QueueName}'.",
                typeof(T),
                subscriptionConfig.TopicName,
                subscriptionConfig.QueueName);
        }


    }
}
