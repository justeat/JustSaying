using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JustSaying.AwsTools.MessageHandling.Dispatch;
using JustSaying.Messaging.Monitoring;
using Microsoft.Extensions.Logging;

namespace JustSaying.Messaging.Channels
{
    internal interface IChannelConsumer
    {
        Task Start(CancellationToken stoppingToken);
        IChannelConsumer ConsumeFrom(IAsyncEnumerable<IQueueMessageContext> messageSource);
    }

    internal class ChannelConsumer : IChannelConsumer
    {
        private IAsyncEnumerable<IQueueMessageContext> _messageSource;
        private readonly IMessageDispatcher _dispatcher;
        private readonly ILogger _logger;

        public ChannelConsumer(IMessageDispatcher dispatcher, ILoggerFactory logger)
        {
            _dispatcher = dispatcher;
            _logger = logger.CreateLogger<IChannelConsumer>();
        }

        public IChannelConsumer ConsumeFrom(IAsyncEnumerable<IQueueMessageContext> messageSource)
        {
            _messageSource = messageSource;
            return this;
        }

        public async Task Start(CancellationToken stoppingToken)
        {
            await Task.Yield();

            await foreach (var messageContext in _messageSource.WithCancellation(stoppingToken))
            {
                await _dispatcher.DispatchMessageAsync(messageContext, stoppingToken).ConfigureAwait(false);
            }
        }
    }
}
