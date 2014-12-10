using JustBehave;
using JustSaying.Messaging.Monitoring;
using NSubstitute;
using NUnit.Framework;

namespace JustSaying.UnitTests.JustSayingFluently.AddingMonitoring
{
    public class WhenAddingACustomMonitor : JustSayingFluentlyTestBase
    {
        readonly IMessageMonitor _monitor = Substitute.For<IMessageMonitor>();
        private object _response;

        protected override void Given() { }

        protected override void When()
        {
            _response = SystemUnderTest.WithMonitoring(_monitor);
        }

        [Then]
        public void ThatMonitorIsAddedToTheStack()
        {
            Bus.Received().Monitor = _monitor;
        }

        [Then]
        public void ICanContinueConfiguringTheBus()
        {
            Assert.IsInstanceOf<IFluentSubscription>(_response);
        }
    }
}