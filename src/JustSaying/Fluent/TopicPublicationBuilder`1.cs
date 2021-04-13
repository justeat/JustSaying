using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon;
using JustSaying.AwsTools;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.AwsTools.QueueCreation;
using JustSaying.Models;
using Microsoft.Extensions.Logging;

namespace JustSaying.Fluent
{
    /// <summary>
    /// A class representing a builder for a topic publication. This class cannot be inherited.
    /// </summary>
    /// <typeparam name="T">
    /// The type of the message.
    /// </typeparam>
    public sealed class TopicPublicationBuilder<T> : IPublicationBuilder<T>
        where T : Message
    {
       /// <summary>
        /// Initializes a new instance of the <see cref="TopicPublicationBuilder{T}"/> class.
        /// </summary>
        internal TopicPublicationBuilder()
        { }

        /// <summary>
        /// Gets or sets a delegate to a method to use to configure SNS writes.
        /// </summary>
        private Action<SnsWriteConfiguration> ConfigureWrites { get; set; }

        /// <summary>
        /// Gets the tags to add to the topic.
        /// </summary>
        private Dictionary<string, string> Tags { get; } = new(StringComparer.Ordinal);

        /// <summary>
        /// Do we override the name of the topic from the infer from type approach
        /// </summary>
        private bool HasNameOverride { get; set; }

        /// <summary>
        /// Do we supply an ARN and not use the name at all?
        /// </summary>
        private bool HasArnNotName { get; set; }

        /// <summary>
        /// What should we do about required infrastructure
        /// </summary>
        private InfrastructureAction InfrastructureAction { get; set; } = InfrastructureAction.CreateIfMissing;

        /// <summary>
        /// The topic is supplied instead of generated from the message type name
        /// </summary>
        private string Topic { get; set; }

        /// <summary>
        /// Configures the SNS write configuration.
        /// </summary>
        /// <param name="configure">A delegate to a method to use to configure SNS writes.</param>
        /// <returns>
        /// The current <see cref="TopicPublicationBuilder{T}"/>.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="configure"/> is <see langword="null"/>.
        /// </exception>
        public TopicPublicationBuilder<T> WithWriteConfiguration(
            Action<SnsWriteConfigurationBuilder> configure)
        {
            if (configure == null)
            {
                throw new ArgumentNullException(nameof(configure));
            }

            var builder = new SnsWriteConfigurationBuilder();

            configure(builder);

            ConfigureWrites = builder.Configure;
            return this;
        }

        /// <summary>
        /// Configures the SNS write configuration.
        /// </summary>
        /// <param name="configure">A delegate to a method to use to configure SNS writes.</param>
        /// <returns>
        /// The current <see cref="TopicPublicationBuilder{T}"/>.
        /// </returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="configure"/> is <see langword="null"/>.
        /// </exception>
        public TopicPublicationBuilder<T> WithWriteConfiguration(Action<SnsWriteConfiguration> configure)
        {
            ConfigureWrites = configure ?? throw new ArgumentNullException(nameof(configure));
            return this;
        }

        /// <summary>
        /// Creates a tag with no value that will be assigned to the SNS topic.
        /// </summary>
        /// <param name="key">The key for the tag.</param>
        /// <returns>
        /// The current <see cref="TopicPublicationBuilder{T}"/>.
        /// </returns>
        /// <remarks>Tag keys are case-sensitive. A new tag with a key identical to that of an existing one will overwrite it.</remarks>
        /// <exception cref="ArgumentException">
        /// <paramref name="key"/> is <see langword="null"/> or whitespace.
        /// </exception>
        public TopicPublicationBuilder<T> WithTag(string key) => WithTag(key, null);

        /// <summary>
        /// Creates a tag with a value that will be assigned to the SNS topic.
        /// </summary>
        /// <param name="key">The key for the tag.</param>
        /// <param name="value">The value associated with this tag.</param>
        /// <returns>
        /// The current <see cref="TopicPublicationBuilder{T}"/>.
        /// </returns>
        /// <remarks>Tag keys are case-sensitive. A new tag with a key identical to that of an existing one will overwrite it.</remarks>
        /// <exception cref="ArgumentException">
        /// <paramref name="key"/> is <see langword="null"/> or whitespace.
        /// </exception>
        public TopicPublicationBuilder<T> WithTag(string key, string value)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                throw new ArgumentException("A topic tag key cannot be null or only whitespace", nameof(key));
            }

            Tags.Add(key, value ?? string.Empty);

            return this;
        }

        /// <summary>
        /// Overrides the policy of generating the topic from the type name, and allows it to be passed in instead
        /// </summary>
        /// <param name="topicName">The topic name to use</param>
        /// <param name="topicARN">We do not supply a name, instead use this externally created Arn instead. Should be used with validate infrastructure only</param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public TopicPublicationBuilder<T> WithTopic(string topicName = null, string topicARN = null)
        {
            bool emptyName = string.IsNullOrEmpty(topicName);
            bool emptyArn = string.IsNullOrEmpty(topicARN);

            if (emptyName && emptyArn)
            {
                throw new InvalidOperationException("You must supply either a topic name or an ARN to use this override");
            }

            //if we supply both, just use the Arn
            if (!emptyArn && !emptyName)
            {
                HasArnNotName = true;
                HasNameOverride = false;
                Topic = topicARN;
            }

            if (!emptyName && emptyArn)
            {
                HasNameOverride = true;
                HasArnNotName = false;
                Topic = topicName;
            }

            if (!emptyArn && emptyName)
            {
                HasArnNotName = true;
                HasNameOverride = false;
                Topic = topicARN;
            }

            return this;

        }

        /// <summary>
        /// We need a topic to send a message to. Do we want to create it, or do we want to validate it exists?
        /// </summary>
        /// <param name="action">The action to take with respect to topics</param>
        /// <returns></returns>
        public TopicPublicationBuilder<T> WithInfastructure(InfrastructureAction action)
        {
            InfrastructureAction = action;
            return this;
        }

        /// <inheritdoc />
        void IPublicationBuilder<T>.Configure(
            JustSayingBus bus,
            IAwsClientFactoryProxy proxy,
            ILoggerFactory loggerFactory)
        {
            var logger = loggerFactory.CreateLogger<TopicPublicationBuilder<T>>();

            logger.LogInformation("Adding SNS publisher for message type '{MessageType}'.",
                typeof(T));

            var config = bus.Config;

            var readConfiguration = new SqsReadConfiguration(SubscriptionType.ToTopic);
            var writeConfiguration = new SnsWriteConfiguration();
            ConfigureWrites?.Invoke(writeConfiguration);

            if (!HasNameOverride && !HasArnNotName)
                readConfiguration.ApplyTopicNamingConvention<T>(config.TopicNamingConvention);
            else if (HasArnNotName || HasNameOverride)
                readConfiguration.TopicName = Topic;

            if (HasArnNotName && InfrastructureAction != InfrastructureAction.ValidateExists)
                throw new Exception($"The only action we can take with an Arn is to validate, not create");

            bus.SerializationRegister.AddSerializer<T>();

            // TODO pass region down into topic creation for when we have foreign topics so we can generate the arn
#pragma warning disable 618
            var eventPublisher = new SnsTopicByName(
                readConfiguration.TopicName,
                proxy.GetAwsClientFactory().GetSnsClient(RegionEndpoint.GetBySystemName(config.Region)),
                bus.SerializationRegister,
                loggerFactory,
                writeConfiguration,
                config.MessageSubjectProvider,
                topicNameIsArn: HasArnNotName)
            {
                MessageResponseLogger = config.MessageResponseLogger,
                Tags = Tags
            };
#pragma warning restore 618

            async Task StartupTask()
            {
                if (InfrastructureAction == InfrastructureAction.CreateIfMissing)
                {

                    if (writeConfiguration.Encryption != null)
                    {
                        await eventPublisher.CreateWithEncryptionAsync(writeConfiguration.Encryption)
                            .ConfigureAwait(false);
                    }
                    else
                    {
                        await eventPublisher.CreateAsync().ConfigureAwait(false);
                    }

                    await eventPublisher.EnsurePolicyIsUpdatedAsync(config.AdditionalSubscriberAccounts)
                        .ConfigureAwait(false);

                    await eventPublisher.ApplyTagsAsync().ConfigureAwait(false);
                }
                else if (InfrastructureAction == InfrastructureAction.ValidateExists)
                {
                    var exists = await eventPublisher.ExistsAsync().ConfigureAwait(false);
                    if (!exists)
                        throw new InvalidOperationException($"The topic {eventPublisher.TopicName} does not exist and infrastructure was set to validate");
                }
            }

            bus.AddStartupTask(StartupTask);

            bus.AddMessagePublisher<T>(eventPublisher);

            logger.LogInformation(
                "Created SNS topic publisher on topic '{TopicName}' for message type '{MessageType}'.",
                readConfiguration.TopicName,
                typeof(T));
        }
    }
}
