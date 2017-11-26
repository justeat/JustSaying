using JustBehave;
using JustSaying.Messaging.MessageSerialisation;
using JustSaying.TestingFramework;
using Xunit;
using Assert = NUnit.Framework.Assert;

namespace JustSaying.Messaging.UnitTests.Serialisation.Newtonsoft
{
    public class WhenSerialisingAndDeserialising : XBehaviourTest<NewtonsoftSerialiser>
    {
        private MessageWithEnum _messageOut;
        private MessageWithEnum _messageIn;
        private string _jsonMessage;
        protected override void Given()
        {
            _messageOut = new MessageWithEnum(Values.Two);
        }

        protected override void When()
        {
            _jsonMessage = SystemUnderTest.Serialise(_messageOut, serializeForSnsPublishing: false);
            _messageIn = SystemUnderTest.Deserialise(_jsonMessage, typeof(MessageWithEnum)) as MessageWithEnum;
        }

        [Fact]
        public void MessageHasBeenCreated()
        {
            Assert.NotNull(_messageOut);
        }

        [Fact]
        public void MessagesContainSameDetails()
        {
            Assert.AreEqual(_messageIn.EnumVal, _messageOut.EnumVal);
            Assert.AreEqual(_messageIn.RaisingComponent, _messageOut.RaisingComponent);
            Assert.AreEqual(_messageIn.TimeStamp, _messageOut.TimeStamp);
        }
        
        [Fact]
        public void EnumsAreRepresentedAsStrings()
        {
            Assert.That(_jsonMessage.Contains("EnumVal"));
            Assert.That(_jsonMessage.Contains("Two"));
        }
    }
}
