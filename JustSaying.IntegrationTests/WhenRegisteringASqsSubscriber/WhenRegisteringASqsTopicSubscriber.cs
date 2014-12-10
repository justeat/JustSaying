﻿using System;
using Amazon;
using Amazon.SQS;
using JustBehave;
using JustSaying.AwsTools;
using JustSaying.Messaging.MessageHandling;
using JustSaying.Messaging.MessageSerialisation;
using JustSaying.TestingFramework;
using JustSaying.Models;
using NSubstitute;
using NUnit.Framework;

namespace JustSaying.IntegrationTests.WhenRegisteringASqsSubscriber
{
    public class WhenRegisteringASqsTopicSubscriber : FluentNotificationStackTestBase
    {
        protected string TopicName;
        protected string QueueName;

        protected override void Given()
        {
            TopicName = "CustomerCommunication";
            QueueName = "queuename-" + DateTime.Now.Ticks;

            EnableMockedBus();

            Configuration = new MessagingConfig();

            DeleteTopicIfItAlreadyExists(TestEndpoint, TopicName);
            DeleteQueueIfItAlreadyExists(TestEndpoint, QueueName);
        }

        protected override void When()
        {
            SystemUnderTest.WithSqsTopicSubscriber()
                .IntoQueue(QueueName)
                .ConfigureSubscriptionWith(cfg =>
            {
                cfg.MessageRetentionSeconds = 60;
            }).WithMessageHandler(Substitute.For<IHandler<Message>>());
        }

        [Then]
        public void SerialisationIsRegisteredForMessage()
        {
            NotificationStack.SerialisationRegister.Received().AddSerialiser<Message>(Arg.Any<IMessageSerialiser<Message>>());
        }

        [Then, Timeout(70000)] // ToDo: Sorry about this, but SQS is a little slow to verify againse. Can be better I'm sure? ;)
        public void QueueIsCreated()
        {
            var queue = new SqsQueueByName(QueueName, new AmazonSQSClient(RegionEndpoint.EUWest1), 0);

            Patiently.AssertThat(queue.Exists, TimeSpan.FromSeconds(65));
        }

        [TearDown]
        public void TearDown()
        {
            DeleteTopicIfItAlreadyExists(TestEndpoint, TopicName);
            DeleteQueueIfItAlreadyExists(TestEndpoint, QueueName);
        }
    }

    public class WhenRegisteringASqsTopicSubscriberUsingBasicSyntax : WhenRegisteringASqsTopicSubscriber
    {
        protected override void When()
        {
            SystemUnderTest.WithSqsTopicSubscriber()
                .IntoQueue(QueueName)
                .WithMessageHandler(Substitute.For<IHandler<Message>>());
        }
    }
}
