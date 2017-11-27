using Amazon.SQS.Model;
using JustSaying.TestingFramework;
using NSubstitute;
using Xunit;

namespace JustSaying.UnitTests.AwsTools.MessageHandling.SqsNotificationListener
{
    public class WhenMessageHandlingThrows : BaseQueuePollingTest
    {
        private bool _firstTime = true;

        protected override void Given()
        {
            base.Given();
            Handler.Handle(Arg.Any<GenericMessage>()).Returns(
                _ => ExceptionOnFirstCall());
        }

        private bool ExceptionOnFirstCall()
        {
            if (_firstTime)
            {
                _firstTime = false;
                throw new TestException("Thrown by test handler");
            }

            return false;
        }

        [Fact]
        public void MessageHandlerWasCalled()
        {
            Handler.ReceivedWithAnyArgs().Handle(Arg.Any<GenericMessage>());
        }

        [Fact]
        public void FailedMessageIsNotRemovedFromQueue()
        {
            Sqs.DidNotReceiveWithAnyArgs().DeleteMessageAsync(Arg.Any<DeleteMessageRequest>());
        }

        [Fact]
        public void ExceptionIsLoggedToMonitor()
        {
            Monitor.ReceivedWithAnyArgs().HandleException(Arg.Any<string>());
        }
    }
}
