using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JustBehave;
using JustSaying.AwsTools.QueueCreation;
using JustSaying.TestingFramework;
using Shouldly;
using Xunit;

namespace JustSaying.IntegrationTests.AwsTools
{
    [Collection(GlobalSetup.CollectionName)]
    public class WhenSettingUpMultipleHandlers : XAsyncBehaviourTest<IHaveFulfilledSubscriptionRequirements>
    {
        private ProxyAwsClientFactory _proxyAwsClientFactory;
        private string _topicName;
        private string _queueName;
        private CancellationTokenSource _subscriberCts;

        protected override void Given()
        {
        }

        protected override Task When()
        {
            return Task.CompletedTask;
        }

        protected override IHaveFulfilledSubscriptionRequirements CreateSystemUnderTest()
        {
            // Given 2 handlers
            var uniqueTopicAndQueueNames = new UniqueNamingStrategy();
            _proxyAwsClientFactory = new ProxyAwsClientFactory();

            var baseQueueName = "CustomerOrders_";
            _topicName = uniqueTopicAndQueueNames.GetTopicName(string.Empty, typeof(Order));
            _queueName = uniqueTopicAndQueueNames.GetQueueName(new SqsReadConfiguration(SubscriptionType.ToTopic) { BaseQueueName = baseQueueName }, typeof(Order));

            var fixture = new JustSayingFixture();

            var subscription = fixture.Builder()
                .WithAwsClientFactory(() => _proxyAwsClientFactory)
                .WithNamingStrategy(() => uniqueTopicAndQueueNames)
                .WithSqsTopicSubscriber()
                .IntoQueue(baseQueueName)
                .WithMessageHandlers(new OrderHandler(), new OrderHandler());

            _subscriberCts = new CancellationTokenSource();
            subscription.StartListening(_subscriberCts.Token);
            return subscription;
        }

        protected override void PostAssertTeardown()
        {
            _subscriberCts.Cancel();
            base.PostAssertTeardown();
        }

        [AwsFact]
        public void CreateTopicCalled()
        {
            _proxyAwsClientFactory.Counters["CreateTopic"][_topicName].Count.ShouldBeGreaterThanOrEqualTo(1);
        }

        [AwsFact]
        public void GetQueueAttributesCalledOnce()
        {
            _proxyAwsClientFactory.Counters["GetQueueAttributes"].First(x => x.Key.EndsWith(_queueName)).Value.Count
                .ShouldBe(1);
        }

        [AwsFact]
        public void CreateQueueCalledOnce()
        {
            AssertHasCounterSetToOne("CreateQueue", _queueName);
        }

        private void AssertHasCounterSetToOne(string counter, string testQueueName)
        {
            var counters = _proxyAwsClientFactory.Counters;

            counters.ShouldContainKey(counter, $"no counter: {counter}");
            counters[counter].ShouldContainKey(testQueueName, $"no queueName: {testQueueName}");
            counters[counter][testQueueName].Count.ShouldBe(1, "Wrong count");
        }
    }
}
