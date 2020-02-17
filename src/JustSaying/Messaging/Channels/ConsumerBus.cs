using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace JustSaying.Messaging.Channels
{
    public interface IConsumerBus
    {
        void AddDownloadBuffer(IDownloadBuffer downloadBuffer);
        void Start(int numberOfConsumers, CancellationToken stoppingToken);
    }

    internal class ConsumerBus : IConsumerBus
    {
        private readonly IConsumerFactory _consumerFactory;
        private readonly IMultiplexer _multiplexer;
        private readonly IList<IDownloadBuffer> _downloadBuffers;
        private readonly ILogger _logger;

        public ConsumerBus(ILoggerFactory loggerFactory, IConsumerFactory consumerFactory)
        {
            _logger = loggerFactory.CreateLogger<ConsumerBus>();
            _consumerFactory = consumerFactory;
            _multiplexer = new RoundRobinQueueMultiplexer(loggerFactory);
            _downloadBuffers = new List<IDownloadBuffer>();
        }

        public void AddDownloadBuffer(IDownloadBuffer downloadBuffer)
        {
            _downloadBuffers.Add(downloadBuffer);
        }

        public void Start(int numberOfConsumers, CancellationToken stoppingToken)
        {
            // link download buffers to core channel
            foreach (var buffer in _downloadBuffers)
            {
                _multiplexer.ReadFrom(buffer.Reader);
            }

            // create n consumers (defined by config)
            // link consumers to core channel
            var consumers = Enumerable.Range(0, numberOfConsumers)
                .Select(x => _consumerFactory.CreateConsumer()
                    .ConsumeFrom(_multiplexer.Messages()));

            _logger.LogInformation("Starting up consumer bus with {ConsumerCount} consumers and {DownloadBufferCount} downloaders",
                 numberOfConsumers, _downloadBuffers.Count);

            // start
            _multiplexer.Start();
            foreach (var consumer in consumers) consumer.Start();
            foreach (var buffer in _downloadBuffers) buffer.Start(stoppingToken);

            _logger.LogInformation("Consumer bus successfully started");
        }
    }
}
