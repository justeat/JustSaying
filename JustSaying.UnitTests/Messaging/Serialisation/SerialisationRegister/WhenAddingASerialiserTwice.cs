using JustBehave;
using JustSaying.Messaging.MessageSerialisation;
using JustSaying.Models;
using NSubstitute;
using Shouldly;
using Xunit;

namespace JustSaying.Messaging.UnitTests.Serialisation.SerialisationRegister
{
    public class WhenAddingASerialiserTwice : XBehaviourTest<MessageSerialisationRegister>
    {
        protected override void Given()
        {
            RecordAnyExceptionsThrown();
        }

        protected override void When()
        {
            SystemUnderTest.AddSerialiser<Message>(Substitute.For<IMessageSerialiser>());
            SystemUnderTest.AddSerialiser<Message>(Substitute.For<IMessageSerialiser>());
        }

        [Fact]
        public void ExceptionIsNotThrown()
        {
            ThrownException.ShouldBeNull();
        }
    }
}
