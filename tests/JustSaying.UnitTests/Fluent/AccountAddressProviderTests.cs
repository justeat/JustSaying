using System;
using JustSaying.Fluent;
using JustSaying.Naming;
using JustSaying.TestingFramework;
using Xunit;

namespace JustSaying.UnitTests.Fluent
{
    public class AccountAddressProviderTests
    {
        [Fact]
        public void CanGetAccountQueueByName()
        {
            var sut = new AccountAddressProvider("123456789012", "eu-west-1");
            var address = sut.GetQueueAddress("queue1");

            Assert.Equal(new Uri(" https://sqs.eu-west-1.amazonaws.com/123456789012/queue1"), address.QueueUrl);
            Assert.Equal("eu-west-1", address.RegionName);
        }

        [Fact]
        public void CanGetAccountTopicByName()
        {
            var sut = new AccountAddressProvider("123456789012", "eu-west-1");
            var address = sut.GetTopicAddress("topic1");

            Assert.Equal("arn:aws:sns:eu-west-1:123456789012:topic1", address.TopicArn);
        }

        [Fact]
        public void CanGetAccountQueueByDefaultConvention()
        {
            var sut = new AccountAddressProvider("123456789012", "eu-west-1");
            var address = sut.GetQueueAddressByConvention<Order>();

            Assert.Equal(new Uri(" https://sqs.eu-west-1.amazonaws.com/123456789012/order"), address.QueueUrl);
            Assert.Equal("eu-west-1", address.RegionName);
        }

        [Fact]
        public void CanGetAccountTopicByDefaultConvention()
        {
            var sut = new AccountAddressProvider("123456789012", "eu-west-1");
            var address = sut.GetTopicAddressByConvention<Order>();

            Assert.Equal("arn:aws:sns:eu-west-1:123456789012:order", address.TopicArn);
        }

        [Fact]
        public void CanGetAccountQueueByCustomConvention()
        {
            var convention = new ManualNamingConvention("adhoc-queue-name", null);
            var sut = new AccountAddressProvider("123456789012", "eu-west-1", convention, null);
            var address = sut.GetQueueAddressByConvention<Order>();

            Assert.Equal(new Uri(" https://sqs.eu-west-1.amazonaws.com/123456789012/adhoc-queue-name"), address.QueueUrl);
            Assert.Equal("eu-west-1", address.RegionName);
        }

        [Fact]
        public void CanGetAccountTopicByCustomConvention()
        {
            var convention = new ManualNamingConvention(null, "adhoc-topic-name");
            var sut = new AccountAddressProvider("123456789012", "eu-west-1", null, convention);
            var address = sut.GetTopicAddressByConvention<Order>();

            Assert.Equal("arn:aws:sns:eu-west-1:123456789012:adhoc-topic-name", address.TopicArn);
        }

        private class ManualNamingConvention : IQueueNamingConvention, ITopicNamingConvention
        {
            private readonly string _queueName;
            private readonly string _topicName;

            public ManualNamingConvention(string queueName, string topicName)
            {
                _queueName = queueName;
                _topicName = topicName;
            }

            public string QueueName<T>() => _queueName;
            public string TopicName<T>() => _topicName;
        }
    }
}
