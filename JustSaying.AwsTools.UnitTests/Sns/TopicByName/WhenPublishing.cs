﻿using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using JustSaying.AwsTools;
using JustSaying.Messaging.MessageSerialisation;
using JustBehave;
using JustSaying.Messaging.Extensions;
using JustSaying.Models;
using JustSaying.TestingFramework;
using NSubstitute;

namespace AwsTools.UnitTests.Sns.TopicByName
{
    public class WhenPublishing : BehaviourTest<SnsTopicByName>
    {
        private const string Message = "the_message_in_json";
        private readonly IMessageSerialisationRegister _serialisationRegister = Substitute.For<IMessageSerialisationRegister>();
        private readonly IAmazonSimpleNotificationService _sns = Substitute.For<IAmazonSimpleNotificationService>();
        private const string TopicName = "topicname";
        private const string TopicArn = "topicarn";

        protected override SnsTopicByName CreateSystemUnderTest()
        {
            return new SnsTopicByName(TopicName, _sns, _serialisationRegister);
        }

        protected override void Given()
        {
            var serialiser = Substitute.For<IMessageSerialiser>();
            serialiser.Serialise(Arg.Any<Message>()).Returns(Message);
            _serialisationRegister.GeTypeSerialiser(typeof(GenericMessage)).Returns(new TypeSerialiser(typeof(GenericMessage), serialiser));
            _sns.FindTopic(TopicName).Returns(new Topic { TopicArn = TopicArn });
        }

        protected override void When()
        {
            SystemUnderTest.Publish(new GenericMessage());
        }

        [Then]
        public void MessageIsPublishedToSnsTopic()
        {
            _sns.Received().Publish(Arg.Is<PublishRequest>(x => x.Message == Message));
        }

        [Then]
        public void MessageSubjectIsObjectType()
        {
            _sns.Received().Publish(Arg.Is<PublishRequest>(x => x.Subject == typeof(GenericMessage).ToKey()));
        }

        [Then]
        public void MessageIsPublishedToCorrectLocation()
        {
            _sns.Received().Publish(Arg.Is<PublishRequest>(x => x.TopicArn == TopicArn));
        }
    }
}
