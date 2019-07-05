using System;
using System.Threading.Tasks;
using JustSaying.AwsTools;
using JustSaying.AwsTools.MessageHandling;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit.Abstractions;

namespace JustSaying.IntegrationTests.Fluent.AwsTools
{
    public class WhenRemovingSnsServerSideEncryption : IntegrationTestBase
    {
        public WhenRemovingSnsServerSideEncryption(ITestOutputHelper outputHelper)
            : base(outputHelper)
        {
        }

        [AwsFact]
        public async Task Can_Remove_Encryption()
        {
            // Arrange
            ILoggerFactory loggerFactory = OutputHelper.ToLoggerFactory();
            IAwsClientFactory clientFactory = CreateClientFactory();

            var client = clientFactory.GetSnsClient(Region);

            var topic = new SnsTopicByName(
                UniqueName,
                client,
                null,
                loggerFactory,
                null);

            await topic.CreateWithEncryptionAsync(new SnsServerSideEncryption { KmsMasterKeyId = JustSayingConstants.DefaultSnsAttributeEncryptionKeyId });

            // Act
            await topic.CreateWithEncryptionAsync(new SnsServerSideEncryption { KmsMasterKeyId = String.Empty });

            // Assert
            topic.ServerSideEncryption.ShouldBeNull();
        }
    }
}
