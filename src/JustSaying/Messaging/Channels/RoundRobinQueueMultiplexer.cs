using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using JustSaying.Messaging.Monitoring;
using Microsoft.Extensions.Logging;

namespace JustSaying.Messaging.Channels
{
    internal sealed class RoundRobinQueueMultiplexer : IMultiplexer, IDisposable
    {
        private readonly IList<ChannelReader<IQueueMessageContext>> _readers;
        private Channel<IQueueMessageContext> _targetChannel;

        private readonly SemaphoreSlim _readersLock = new SemaphoreSlim(1, 1);
        private readonly SemaphoreSlim _startLock = new SemaphoreSlim(1, 1);
        readonly ILogger<RoundRobinQueueMultiplexer> _logger;

        private bool _started = false;
        private int _channelCapacity;
        private CancellationToken _stoppingToken;

        public Task Completion { get; private set; }

        public RoundRobinQueueMultiplexer(ILoggerFactory loggerFactory)
        {
            _readers = new List<ChannelReader<IQueueMessageContext>>();
            _logger = loggerFactory.CreateLogger<RoundRobinQueueMultiplexer>();

            // TODO: make configurable
            _channelCapacity = 100;
            _targetChannel = Channel.CreateBounded<IQueueMessageContext>(_channelCapacity);
        }

        public void ReadFrom(ChannelReader<IQueueMessageContext> reader)
        {
            if (reader == null) throw new ArgumentNullException(nameof(reader));

            _readersLock.Wait();
            try
            {
                _readers.Add(reader);
            }
            finally
            {
                _readersLock.Release();
            }

            reader.Completion.ContinueWith(c => RemoveReader(reader), TaskScheduler.Default);
        }

        private void RemoveReader(ChannelReader<IQueueMessageContext> reader)
        {
            _logger.LogInformation("Received notification to remove reader from multiplexer inputs");

            _readersLock.Wait();
            try
            {
                _readers.Remove(reader);
            }
            finally
            {
                _readersLock.Release();
            }
        }

        public async Task Start(CancellationToken stoppingToken = default)
        {
            if (_started) return;

            await _startLock.WaitAsync().ConfigureAwait(false);

            if (_started) return;
            _started = true;

            _stoppingToken = stoppingToken;

            try
            {
                Completion = Run(stoppingToken);
            }
            finally
            {
                _startLock.Release();
            }
        }

        private async Task Run(CancellationToken stoppingToken)
        {
            await Task.Yield();

            _logger.LogInformation("Starting up channel multiplexer with a queue capacity of {Capacity}",
                _channelCapacity);

            var writer = _targetChannel.Writer;
            while (true)
            {
                await _readersLock.WaitAsync().ConfigureAwait(false);

                try
                {
                    if (_readers.Count < 1)
                    {
                        _logger.LogInformation("All writers have completed, terminating multiplexer");
                        writer.Complete();
                        break;
                    }

                    foreach (var reader in _readers)
                    {
                        if (reader.TryRead(out var message))
                        {
                            await writer.WriteAsync(message);
                        }
                    }

                    stoppingToken.ThrowIfCancellationRequested();
                }
                finally
                {
                    _readersLock.Release();
                }
            }
        }

        public async IAsyncEnumerable<IQueueMessageContext> Messages()
        {
            await Start().ConfigureAwait(false);

            while (true)
            {
                var couldWait = await _targetChannel.Reader.WaitToReadAsync(_stoppingToken);
                if (!couldWait) break;

                _stoppingToken.ThrowIfCancellationRequested();

                while (_targetChannel.Reader.TryRead(out var message))
                    yield return message;
            }
        }

        public void Dispose()
        {
            _startLock.Dispose();
            _readersLock.Dispose();
        }
    }
}
