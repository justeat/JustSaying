﻿using System.Threading.Tasks;
using JustBehave;
using JustSaying.AwsTools.QueueCreation;
using NUnit.Framework;
using JustSaying.TestingFramework;

namespace JustSaying.AwsTools.IntegrationTests
{
    public class WhenICreateAQueueByName : WhenCreatingQueuesByName
    {
        private bool _isQueueCreated;

        protected override void When()
        {
            _isQueueCreated = SystemUnderTest.Create(new SqsBasicConfiguration(), attempt: 0);
        }

        [Then]
        public void TheQueueISCreated()
        {
            Assert.IsTrue(_isQueueCreated);
        }

        [Then, Explicit("Extremely long running test")]
        public async Task DeadLetterQueueIsCreated()
        {
            await Patiently.AssertThatAsync(
                () => SystemUnderTest.ErrorQueue.Exists(),
                40.Seconds());
        }
    }
}
