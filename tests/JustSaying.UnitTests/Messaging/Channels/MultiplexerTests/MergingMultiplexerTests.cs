using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using JustSaying.Messaging.Channels.Context;
using JustSaying.Messaging.Channels.Multiplexer;
using Xunit;
using Xunit.Abstractions;

namespace JustSaying.UnitTests.Messaging.Channels.MultiplexerTests
{
    public class MergingMultiplexerTests
    {
        private readonly ITestOutputHelper _outputHelper;
        private static readonly TimeSpan TimeoutPeriod = TimeSpan.FromMilliseconds(100);

        public MergingMultiplexerTests(ITestOutputHelper outputHelper)
        {
            _outputHelper = outputHelper;
        }

        [Fact]
        public void Starting_Twice_Returns_Same_Task()
        {
            // Arrange
            using var multiplexer = new MergingMultiplexer(10, _outputHelper.ToLogger<MergingMultiplexer>());

            var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeoutPeriod);

            // Act
            Task completed1 = multiplexer.Run(cts.Token);
            Task completed2 = multiplexer.Run(cts.Token);

            // Assert
            Assert.Equal(completed1, completed2);
        }

        [Fact]
        public void Cannot_Add_Invalid_Reader()
        {
            // Arrange
            using var multiplexer = new MergingMultiplexer(10, _outputHelper.ToLogger<MergingMultiplexer>());

            // Act and Assert
            Assert.Throws<ArgumentNullException>(() => multiplexer.ReadFrom(null));
        }

        [Fact]
        public void Cannot_Get_Messages_Before_Started()
        {
            // Arrange
            using var multiplexer = new MergingMultiplexer(10, _outputHelper.ToLogger<MergingMultiplexer>());

            // Act and Assert
            Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await foreach (var msg in multiplexer.GetMessagesAsync())
                {
                }
            });
        }

        [Fact]
        public async Task Reader_Completes_And_Is_Removed()
        {
            // Arrange
            using var multiplexer = new MergingMultiplexer(10, _outputHelper.ToLogger<MergingMultiplexer>());

            var cts = new CancellationTokenSource();

            var channel = Channel.CreateBounded<IQueueMessageContext>(10);
            multiplexer.ReadFrom(channel);

            // Act
            Task completed = multiplexer.Run(cts.Token);
            channel.Writer.Complete();

            // Assert
            await completed;
        }
    }
}