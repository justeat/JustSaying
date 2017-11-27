using System.Threading.Tasks;
using JustSaying.AwsTools.QueueCreation;
using Xunit;
using JustSaying.TestingFramework;

namespace JustSaying.IntegrationTests.AwsTools
{
    [Collection(GlobalSetup.CollectionName)]
    public class WhenIAccessAnExistingQueueWithoutAnErrorQueue : WhenCreatingQueuesByName
    {
        protected override Task When()
        {
            SystemUnderTest.Create(new SqsBasicConfiguration {ErrorQueueOptOut = true}, attempt: 0);
            return Task.CompletedTask;
        }

        [Fact]
        public async Task ThereIsNoErrorQueue()
        {
            await Patiently.AssertThatAsync(() => !SystemUnderTest.ErrorQueue.Exists());
        }
    }
}
