using System;
using System.Collections.Generic;
using JustSaying.AwsTools.MessageHandling;

namespace JustSaying.Messaging.Channels.SubscriptionGroups
{
    public class SubscriptionGroupSettings
    {
        internal SubscriptionGroupSettings(
            string name,
            int concurrencyLimit,
            int bufferSize,
            TimeSpan receiveBufferReadTimeout,
            TimeSpan receiveBufferWriteTimeout,
            int multiplexerCapacity,
            int prefetch,
            IReadOnlyCollection<ISqsQueue> queues)
        {
            ConcurrencyLimit = concurrencyLimit;
            BufferSize = bufferSize;
            ReceiveBufferReadTimeout = receiveBufferReadTimeout;
            ReceiveBufferWriteTimeout = receiveBufferWriteTimeout;
            MultiplexerCapacity = multiplexerCapacity;
            Prefetch = prefetch;
            Queues = queues;
            Name = name;
        }

        public int ConcurrencyLimit { get; }
        public int BufferSize { get; }
        public TimeSpan ReceiveBufferReadTimeout { get; }
        public TimeSpan ReceiveBufferWriteTimeout { get; }
        public int MultiplexerCapacity { get; }
        public int Prefetch { get; }
        public string Name { get; }
        public IReadOnlyCollection<ISqsQueue> Queues { get; }
    }
}