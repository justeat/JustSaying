using Newtonsoft.Json;

namespace JustSaying.AwsTools.QueueCreation
{
    public class RedrivePolicy
    {
        [JsonProperty(PropertyName = "maxReceiveCount")]
        public int MaximumReceives { get; set; }

        [JsonProperty(PropertyName = "deadLetterTargetArn")]
        public string DeadLetterQueue { get; set; }

        public RedrivePolicy(int maximumReceives, string deadLetterQueue)
        {
            MaximumReceives = maximumReceives;
            DeadLetterQueue = deadLetterQueue;
        }

        protected RedrivePolicy() { }

        public override string ToString()
            => "{\"maxReceiveCount\":\"" + MaximumReceives + "\", \"deadLetterTargetArn\":\"" + DeadLetterQueue + "\"}";

        public static RedrivePolicy ConvertFromString(string policy)
            => JsonConvert.DeserializeObject<RedrivePolicy>(policy);
    }
}