using System.Threading.Tasks;
using JustSaying.Messaging.MessageHandling;
using JustSaying.TestingFramework;

namespace JustSaying.AwsTools.UnitTests.MessageHandling.SqsNotificationListener.Support
{
    [ExactlyOnce(TimeOut = 5)]
    public class ExplicitExactlyOnceSignallingHandler : IHandlerAsync<GenericMessage>
    {
        private readonly TaskCompletionSource<object> _doneSignal;

        public ExplicitExactlyOnceSignallingHandler(TaskCompletionSource<object> doneSignal)
        {
            _doneSignal = doneSignal;
        }

        public Task<bool> Handle(GenericMessage message)
        {
            HandleWasCalled = true;
            Tasks.DelaySendDone(_doneSignal);
            return Task.FromResult(true);
        }

        public bool HandleWasCalled { get; private set; }
    }
}