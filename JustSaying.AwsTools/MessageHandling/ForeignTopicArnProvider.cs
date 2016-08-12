using Amazon;

namespace JustSaying.AwsTools.MessageHandling
{
    public class ForeignTopicArnProvider : ITopicArnProvider
    {
        
        private readonly string _arn;

        public ForeignTopicArnProvider(RegionEndpoint regionEndpoint, string accountId, string topicName)
        {
            _arn = $"arn:aws:sns:{regionEndpoint.SystemName}:{accountId}:{topicName}";
        }

        public bool ArnExists()
        {
            // Assume foreign topics exist, we actually find out when we attempt to subscribe
            return true;
        }

        public string GetArn()
        {
            return _arn;
        }
    }
}
