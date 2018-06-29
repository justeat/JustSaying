using JustBehave;
using JustSaying.AwsTools;
using JustSaying.TestingFramework;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace JustSaying.UnitTests.JustSayingFluently.RegisteringPublishers
{
    public class WhenAddingPublishers : XBehaviourTest<JustSaying.JustSayingFluently>
    {
        private readonly IAmJustSaying _bus = Substitute.For<IAmJustSaying>();

        protected override JustSaying.JustSayingFluently CreateSystemUnderTest()
        {
            return new JustSaying.JustSayingFluently(_bus, null, new AwsClientFactoryProxy(), Substitute.For<ILoggerFactory>());
        }

        protected override void Given()
        {
            RecordAnyExceptionsThrown();

            var config = new MessagingConfig();
            config.Regions.Add("fake_region");
            _bus.Config.Returns(config);
        }

        protected override void When()
        {
        }

        [Fact]
        public void ConfigurationIsRequired()
        {
            SystemUnderTest.ConfigurePublisherWith(conf => conf.PublishFailureBackoffMilliseconds = 50);
        }

        /// Note: Ignored tests are here for fluent api exploration & expecting compile time issues when working on the fluent interface stuff...
        [Fact(Skip = "Testing compile-time issues")]
        public void ASnsPublisherCanBeSetup()
        {
            SystemUnderTest.ConfigurePublisherWith(conf => conf.PublishFailureBackoffMilliseconds = 50)
                .WithSnsMessagePublisher<GenericMessage>();
        }

        [Fact(Skip = "Testing compile-time issues")]
        public void MultipleSnsPublishersCanBeSetup()
        {
            SystemUnderTest.ConfigurePublisherWith(conf => conf.PublishFailureBackoffMilliseconds = 50)
                .WithSnsMessagePublisher<GenericMessage>()
                .WithSnsMessagePublisher<GenericMessage>();
        }

        [Fact(Skip = "Testing compile-time issues")]
        public void ASqsPublisherCanBeSetupWithConfiguration()
        {
            SystemUnderTest.WithSqsMessagePublisher<GenericMessage>(c =>
            {
                c.VisibilityTimeoutSeconds = 1;
                c.RetryCountBeforeSendingToErrorQueue = 2;
                c.MessageRetentionSeconds = 3;
                c.ErrorQueueOptOut = true;
            });
        }
    }
}
