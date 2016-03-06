using System.Threading.Tasks;
using JustSaying.Messaging.MessageHandling;
using JustSaying.TestingFramework;

namespace JustSaying.AwsTools.UnitTests.MessageHandling.SqsNotificationListener.Support
{
    [ExactlyOnce(TimeOut = 5)]
    public class ExplicitExactlyOnceSignallingHandler : IHandler<GenericMessage>
    {
        private bool _handlerWasCalled;
        private readonly TaskCompletionSource<object> _doneSignal;

        public ExplicitExactlyOnceSignallingHandler(TaskCompletionSource<object> doneSignal)
        {
            _doneSignal = doneSignal;
        }

        public bool Handle(GenericMessage message)
        {
            _handlerWasCalled = true;
            Tasks.DelaySendDone(_doneSignal);
            return true;
        }

        public bool HandlerWasCalled()
        {
            return _handlerWasCalled;
        }
    }
}