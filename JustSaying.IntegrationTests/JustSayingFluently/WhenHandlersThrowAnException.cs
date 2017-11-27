using System.Threading.Tasks;
using JustSaying.IntegrationTests.TestHandlers;
using JustSaying.TestingFramework;
using NSubstitute;
using Shouldly;
using Xunit;

namespace JustSaying.IntegrationTests.JustSayingFluently
{
    [Collection(GlobalSetup.CollectionName)]
    public class WhenHandlersThrowAnException : GivenANotificationStack
    {
        private Future<GenericMessage> _handler;

        protected override void Given()
        {
            RecordAnyExceptionsThrown();

            base.Given();
            _handler = new Future<GenericMessage>(() => throw new TestException("Test Exception from WhenHandlersThrowAnException"));
            RegisterSnsHandler(_handler);
        }

        protected override async Task When()
        {
            await ServiceBus.PublishAsync(new GenericMessage());
            await _handler.DoneSignal;
        }

        [Fact]
        public void ThenExceptionIsRecordedInMonitoring()
        {
            _handler.ReceivedMessageCount.ShouldBeGreaterThan(0);

            Monitoring.Received().HandleException(Arg.Any<string>());
        }
    }
}
