using System;
using System.Collections.Generic;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.AwsTools.QueueCreation;
using JustSaying.Messaging.Middleware;
using JustSaying.Models;
using JustSaying.Naming;
using Microsoft.Extensions.Logging;

using HandleMessageMiddleware = JustSaying.Messaging.Middleware.MiddlewareBase<JustSaying.Messaging.Middleware.HandleMessageContext, bool>;

namespace JustSaying.Fluent
{
    /// <summary>
    /// A class representing a builder for a queue subscription. This class cannot be inherited.
    /// </summary>
    /// <typeparam name="T">
    /// The type of the message.
    /// </typeparam>
    public sealed class QueueSubscriptionBuilder<T> : ISubscriptionBuilder<T>
        where T : Message
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="QueueSubscriptionBuilder{T}"/> class.
        /// </summary>
        internal QueueSubscriptionBuilder()
        { }

        /// <summary>
        /// Gets or sets the queue queueName.
        /// </summary>
        private string QueueName { get; set; } = string.Empty;

        /// <summary>
        /// Gets or sets the topic queueName or Arn.
        /// </summary>
        private string Topic { get; set; } = string.Empty;


        /// <summary>
        /// Gets or sets a delegate to a method to use to configure SQS reads.
        /// </summary>
        private Action<SqsReadConfiguration> ConfigureReads { get; set; }

        /// <summary>
        /// Is the queue named via a naming strategy via the subscription data type (true) or passed in (false)
        /// </summary>
        private bool HasDefaultQueueName { get; set; }

        /// <summary>
        /// Just use the naming convention to create the topic queueName from type queueName
        /// </summary>
        private bool HasDefaultTopicName { get; set; }

        /// <summary>
        /// Do we override the queueName of the topic from the infer from type approach
        /// </summary>
        private bool HasNameOverride { get; set; }

        /// <summary>
        /// Do we supply an ARN and not use the queueName at all?
        /// </summary>
        private bool HasArnNotName { get; set; }

        /// <summary>
        /// What should we do about required infrastructure
        /// </summary>
        private InfrastructureAction InfrastructureAction { get; set; } = InfrastructureAction.CreateIfMissing;

        /// <summary>
        /// Gets the tags to add to the queue.
        /// </summary>
        private Dictionary<string, string> Tags { get; } = new(StringComparer.Ordinal);

        /// <summary>
        /// Configures that the <see cref="IQueueNamingConvention"/> will create the queue queueName that should be used.
        /// </summary>
        /// <returns>
        /// The current <see cref="QueueSubscriptionBuilder{T}"/>.
        /// </returns>
        public QueueSubscriptionBuilder<T> WithDefaultQueue()
            => WithQueue(string.Empty);

        /// <summary>
        /// What should we do about infrastructure requirements
        /// </summary>
        /// <param name="action">What action should we take</param>
        /// <returns></returns>
        public QueueSubscriptionBuilder<T> WithInfrastructure(InfrastructureAction action)
        {
            InfrastructureAction = action;
            return this;
        }

        /// <summary>
        /// Configures the queueName of the queue.
        /// </summary>
        /// <param name="queueName">The queueName of the queue to subscribe to.</param>
        /// <returns>
        /// The current <see cref="QueueSubscriptionBuilder{T}"/>.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="queueName"/> is <see langword="null"/>.
        /// </exception>
        public QueueSubscriptionBuilder<T> WithQueue(string queueName)
        {
            if (string.IsNullOrEmpty(queueName))
            {
                HasDefaultQueueName = true;
            }
            else
            {
                QueueName = queueName;
                HasDefaultQueueName = false;
            }


            return this;
        }

        /// <summary>
        /// Configures the SQS read configuration.
        /// </summary>
        /// <param name="configure">A delegate to a method to use to configure SQS reads.</param>
        /// <returns>
        /// The current <see cref="QueueSubscriptionBuilder{T}"/>.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="configure"/> is <see langword="null"/>.
        /// </exception>
        public QueueSubscriptionBuilder<T> WithReadConfiguration(
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
        /// Configures the SQS read configuration.
        /// </summary>
        /// <param name="configure">A delegate to a method to use to configure SQS reads.</param>
        /// <returns>
        /// The current <see cref="QueueSubscriptionBuilder{T}"/>.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="configure"/> is <see langword="null"/>.
        /// </exception>
        public QueueSubscriptionBuilder<T> WithReadConfiguration(Action<SqsReadConfiguration> configure)
        {
            ConfigureReads = configure ?? throw new ArgumentNullException(nameof(configure));
            return this;
        }

        /// <summary>
        /// Overrides the policy of generating the topic from the type queueName, and allows it to be passed in instead
        /// </summary>
        /// <param name="topicName">The topic queueName to use</param>
        /// <param name="topicARN">We do not supply a queueName, instead use this externally created Arn instead. Should be used with validate infrastructure only</param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public QueueSubscriptionBuilder<T> WithTopic(string topicName = null, string topicARN = null)
        {
            bool emptyName = string.IsNullOrEmpty(topicName);
            bool emptyArn = string.IsNullOrEmpty(topicARN);

            if (!emptyArn && !emptyName)
            {
                HasDefaultTopicName = true;
            }

            //if we supply both, just use the Arn
            if (!emptyArn)
            {
                HasArnNotName = true;
                Topic = topicARN;
            }

            if (!emptyName)
            {
                HasNameOverride = true;
                Topic = topicName;
            }

            return this;

        }

        /// <summary>
        /// Creates a tag with no value that will be assigned to the SQS queue.
        /// </summary>
        /// <param name="key">The key for the tag.</param>
        /// <returns>
        /// The current <see cref="QueueSubscriptionBuilder{T}"/>.
        /// </returns>
        /// <remarks>Tag keys are case-sensitive. A new tag with a key identical to that of an existing one will overwrite it.</remarks>
        /// <exception cref="ArgumentException">
        /// <paramref name="key"/> is <see langword="null"/> or whitespace.
        /// </exception>
        public QueueSubscriptionBuilder<T> WithTag(string key) => WithTag(key, null);

        /// <summary>
        /// Creates a tag with a value that will be assigned to the SQS queue.
        /// </summary>
        /// <param name="key">The key for the tag.</param>
        /// <param name="value">The value associated with this tag.</param>
        /// <returns>
        /// The current <see cref="QueueSubscriptionBuilder{T}"/>.
        /// </returns>
        /// <remarks>Tag keys are case-sensitive. A new tag with a key identical to that of an existing one will overwrite it.</remarks>
        /// <exception cref="ArgumentException">
        /// <paramref name="key"/> is <see langword="null"/> or whitespace.
        /// </exception>
        public QueueSubscriptionBuilder<T> WithTag(string key, string value)
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
            var logger = loggerFactory.CreateLogger<QueueSubscriptionBuilder<T>>();

            var subscriptionConfig = new SqsReadConfiguration(SubscriptionType.PointToPoint)
            {
                QueueName = QueueName,
                TopicName = Topic,
                Tags = Tags
            };

            ConfigureReads?.Invoke(subscriptionConfig);

            var config = bus.Config;

            if (HasDefaultTopicName)
            {
                subscriptionConfig.ApplyTopicNamingConvention<T>(config.TopicNamingConvention);
            }

            if (HasDefaultQueueName)
            {
                subscriptionConfig.ApplyQueueNamingConvention<T>(config.QueueNamingConvention);
            }

            subscriptionConfig.SubscriptionGroupName ??= subscriptionConfig.QueueName;
            subscriptionConfig.MiddlewareConfiguration = subscriptionConfig.MiddlewareConfiguration;
            subscriptionConfig.Validate();

            var queue = creator.EnsureQueueExists(config.Region, HasArnNotName, subscriptionConfig);
            bus.AddStartupTask(queue.StartupTask);

            bus.AddQueue(subscriptionConfig.SubscriptionGroupName, queue.Queue);

            logger.LogInformation(
                "Created SQS subscriber for message type '{MessageType}' on queue '{QueueName}'.",
                typeof(T),
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
                "Added a message handler for message type for '{MessageType}' on topic '{TopicName}' and queue '{QueueName}'.",
                typeof(T),
                subscriptionConfig.TopicName,
                subscriptionConfig.QueueName);
        }
   }
}
