using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;

namespace JustSaying.AwsTools.MessageHandling
{
    internal class ForeignTopicArnProvider : ITopicArnProvider
    {
        private IAmazonSimpleNotificationService _client;

        private readonly string _arn;

        public ForeignTopicArnProvider(
            string topicArn,
            IAmazonSimpleNotificationService client)
        {
            _arn = topicArn;
            _client = client;
        }

        public ForeignTopicArnProvider(
            RegionEndpoint regionEndpoint,
            string accountId,
            string topicName,
            IAmazonSimpleNotificationService client)
        {
            _arn = $"arn:aws:sns:{regionEndpoint.SystemName}:{accountId}:{topicName}";
            _client = client;
        }

        public async Task<bool> ArnExistsAsync()
        {
            bool exists = false;
            ListTopicsResponse response;
            do
            {
                response = await _client.ListTopicsAsync();
                exists = response.Topics.Any(topic => topic.TopicArn == _arn);
            } while (!exists && response.NextToken != null);

            return exists;
        }

        public Task<string> GetArnAsync()
        {
            return Task.FromResult(_arn);
        }
    }
}
