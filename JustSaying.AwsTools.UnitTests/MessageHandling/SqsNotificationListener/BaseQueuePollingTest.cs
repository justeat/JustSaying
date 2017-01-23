using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using JustBehave;
using JustSaying.AwsTools.MessageHandling;
using JustSaying.AwsTools.UnitTests.MessageHandling.SqsNotificationListener.Support;
using JustSaying.Messaging.MessageHandling;
using JustSaying.Messaging.MessageSerialisation;
using JustSaying.Messaging.Monitoring;
using JustSaying.TestingFramework;
using Microsoft.Extensions.Logging;
using NSubstitute;
using NUnit.Framework;

namespace JustSaying.AwsTools.UnitTests.MessageHandling.SqsNotificationListener
{
    public abstract class BaseQueuePollingTest : AsyncBehaviourTest<AwsTools.MessageHandling.SqsNotificationListener>
    {
        protected const string QueueUrl = "url";
        protected IAmazonSQS Sqs;
        protected GenericMessage DeserialisedMessage;
        protected const string MessageBody = "object";
        protected IHandlerAsync<GenericMessage> Handler;
        protected IMessageMonitor Monitor;
        protected ILoggerFactory LoggerFactory;
        protected IMessageSerialisationRegister SerialisationRegister;
        protected IMessageLock MessageLock;
        protected readonly string MessageTypeString = typeof(GenericMessage).ToString();

        protected override AwsTools.MessageHandling.SqsNotificationListener CreateSystemUnderTest()
        {
            var queue = new SqsQueueByUrl(RegionEndpoint.EUWest1, QueueUrl, Sqs);
            return new AwsTools.MessageHandling.SqsNotificationListener(queue, SerialisationRegister, Monitor, LoggerFactory, null, MessageLock);
        }

        protected override void Given()
        {
            LoggerFactory = new LoggerFactory().AddConsole();
            Sqs = Substitute.For<IAmazonSQS>();
            SerialisationRegister = Substitute.For<IMessageSerialisationRegister>();
            Monitor = Substitute.For<IMessageMonitor>();
            Handler = Substitute.For<IHandlerAsync<GenericMessage>>();
            LoggerFactory = Substitute.For<ILoggerFactory>();
            
            var response = GenerateResponseMessage(MessageTypeString, Guid.NewGuid());
            
            Sqs.ReceiveMessageAsync(
                    Arg.Any<ReceiveMessageRequest>(), 
                    Arg.Any<CancellationToken>())
                .Returns(
                    x => Task.FromResult(response),
                    x => Task.FromResult(new ReceiveMessageResponse()));

            DeserialisedMessage = new GenericMessage { RaisingComponent = "Component" };
            SerialisationRegister.DeserializeMessage(Arg.Any<string>()).Returns(DeserialisedMessage);
        }
        protected override async Task When()
        {
            var doneSignal = new TaskCompletionSource<object>();
            var signallingHandler = new SignallingHandler<GenericMessage>(doneSignal, Handler);

            SystemUnderTest.AddMessageHandler(() => signallingHandler);
            SystemUnderTest.Listen();

            // wait until it's done
            var doneOk = await Tasks.WaitWithTimeoutAsync(doneSignal.Task);

            SystemUnderTest.StopListening();

            Assert.IsTrue(doneOk, "Timout occured before done signal");
        }

        protected ReceiveMessageResponse GenerateResponseMessage(string messageType, Guid messageId)
        {
            return new ReceiveMessageResponse
            {
                Messages = new List<Message>
                {
                    new Message
                    {   
                        MessageId = messageId.ToString(),
                        Body = SqsMessageBody(messageType)
                    },
                    new Message
                    {
                        MessageId = messageId.ToString(),
                        Body = "{\"Subject\":\"SOME_UNKNOWN_MESSAGE\"," + "\"Message\":\"SOME_RANDOM_MESSAGE\"}"
                    }
                }
            };
        }

        protected string SqsMessageBody(string messageType)
        {
            return "{\"Subject\":\"" + messageType + "\"," + "\"Message\":\"" + MessageBody + "\"}";
        }
    }
}