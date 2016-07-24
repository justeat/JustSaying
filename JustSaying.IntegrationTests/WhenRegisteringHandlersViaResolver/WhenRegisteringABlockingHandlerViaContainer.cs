using System.Linq;
using JustSaying.IntegrationTests.TestHandlers;
using JustSaying.Messaging.MessageHandling;
using NUnit.Framework;
using Shouldly;
using StructureMap;

namespace JustSaying.IntegrationTests.WhenRegisteringHandlersViaResolver
{
    public class WhenRegisteringABlockingHandlerViaContainer : GivenAPublisher
    {
        private BlockingOrderProcessor _resolvedHandler;

        protected override void Given()
        {
           var container = new Container(x => x.AddRegistry(new BlockingHandlerRegistry()));

           var handlerResolver = new StructureMapHandlerResolver(container);
            var handlers = handlerResolver.ResolveHandlers<OrderPlaced>().ToList();
            Assert.That(handlers.Count, Is.EqualTo(1));

            var blockingHandler = (BlockingHandler<OrderPlaced>)handlers[0];
            _resolvedHandler = (BlockingOrderProcessor)blockingHandler.Inner;
            DoneSignal = _resolvedHandler.DoneSignal.Task;

            Subscriber = CreateMeABus.InRegion("eu-west-1")
                .WithSqsTopicSubscriber()
                .IntoQueueNamed("container-test")
                .WithMessageHandler<OrderPlaced>(handlerResolver);

            Subscriber.StartListening();
        }

        [Test]
        public void ThenHandlerWillReceiveTheMessage()
        {
            _resolvedHandler.ReceivedMessageCount.ShouldBeGreaterThan(0);
        }
    }
}