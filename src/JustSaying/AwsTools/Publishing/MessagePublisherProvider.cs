using System;
using System.Collections.Generic;
using System.Threading;
using Amazon;
using Amazon.SimpleNotificationService;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.AwsTools.QueueCreation;
using JustSaying.Messaging;
using JustSaying.Messaging.MessageSerialization;
using Microsoft.Extensions.Logging;

namespace JustSaying.AwsTools.Publishing
{
    /// <summary>
    /// Provides <see cref="IMessagePublisher"/>'s without having to know the details of how to create them.
    /// Multiple calls to get a publisher for the same queue or topic will return the same publisher.
    /// </summary>
    public class MessagePublisherProvider : IMessagePublisherFactory, IQueueTopicCreatorProvider
    {
        private readonly IAwsClientFactoryProxy _proxy;
        private readonly IMessageSerializationRegister _serializationRegister;
        private readonly ILoggerFactory _loggerFactory;
        private readonly IMessagingConfig _config;

        private readonly Dictionary<string, SqsPublisher> _queuePublishers = new();
        private readonly Dictionary<string, SnsTopicByName> _topicPublishers = new();

        private readonly ReaderWriterLockSlim _writeLock = new();

        public MessagePublisherProvider(
            IAwsClientFactoryProxy proxy,
            IMessageSerializationRegister serializationRegister,
            ILoggerFactory loggerFactory,
            IMessagingConfig config)
        {
            _proxy = proxy;
            _serializationRegister = serializationRegister;
            _loggerFactory = loggerFactory;
            _config = config;
        }

        ///<inheritdoc/>
        public IMessagePublisher GetSnsPublisher(string topicName, IDictionary<string, string> tags)
        {
            return GetTopicPublisher(topicName, tags);
        }

        ///<inheritdoc/>
        public IMessagePublisher GetSqsPublisher(string queueName, int retryCountBeforeSendingToErrorQueue)
        {
            return GetQueuePublisher(queueName, _config.Region, retryCountBeforeSendingToErrorQueue);
        }

        ///<inheritdoc/>
        public ITopicCreator GetSnsCreator(string topicName, IDictionary<string, string> tags)
        {
            return GetTopicPublisher(topicName, tags);
        }

        ///<inheritdoc/>
        public IQueueCreator GetSqsCreator(
            string queueName,
            string region,
            int retryCountBeforeSendingToErrorQueue,
            Dictionary<string, string> tags)
        {
            return GetQueuePublisher(queueName, region, retryCountBeforeSendingToErrorQueue);
        }

        private SqsPublisher GetQueuePublisher(string queueName, string region, int retryCountBeforeSendingToErrorQueue)
        {
            _writeLock.EnterUpgradeableReadLock();

            try
            {
                if (_queuePublishers.ContainsKey(queueName))
                {
                    return _queuePublishers[queueName];
                }
                else
                {
                    try
                    {
                        _writeLock.EnterWriteLock();

                        if (_queuePublishers.ContainsKey(queueName))
                        {
                            return _queuePublishers[queueName];
                        }

                        var regionEndpoint = RegionEndpoint.GetBySystemName(region);
                        var client = _proxy.GetAwsClientFactory().GetSqsClient(regionEndpoint);

                        _queuePublishers[queueName] = new SqsPublisher(regionEndpoint, queueName, client, retryCountBeforeSendingToErrorQueue, _serializationRegister, _loggerFactory)
                        {
                            MessageResponseLogger = _config.MessageResponseLogger
                        };

                        return _queuePublishers[queueName];
                    }
                    finally
                    {
                        _writeLock.ExitWriteLock();
                    }
                }
            }
            finally
            {
                _writeLock.ExitUpgradeableReadLock();
            }
        }

        private SnsTopicByName GetTopicPublisher(string topicName, IDictionary<string, string> tags)
        {
            _writeLock.EnterUpgradeableReadLock();

            try
            {
                if (_topicPublishers.ContainsKey(topicName))
                {
                    return _topicPublishers[topicName];
                }
                else
                {
                    _writeLock.EnterWriteLock();

                    if (_topicPublishers.ContainsKey(topicName))
                    {
                        return _topicPublishers[topicName];
                    }

                    try
                    {
                        _topicPublishers[topicName] = new SnsTopicByName(topicName,
                            _proxy.GetAwsClientFactory().GetSnsClient(RegionEndpoint.GetBySystemName(_config.Region)),
                            _serializationRegister,
                            _loggerFactory,
                            _config.MessageSubjectProvider,
                            null,
                            tags);

                        return _topicPublishers[topicName];
                    }
                    finally
                    {
                        _writeLock.ExitWriteLock();
                    }
                }
            }
            finally
            {
                _writeLock.ExitUpgradeableReadLock();
            }
        }
    }
}
