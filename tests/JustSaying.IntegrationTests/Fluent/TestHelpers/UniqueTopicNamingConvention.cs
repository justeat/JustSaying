using System;
using System.Text.RegularExpressions;
using JustSaying.Naming;

namespace JustSaying.IntegrationTests.Fluent.Subscribing
{
    public class UniqueTopicNamingConvention : ITopicNamingConvention
    {
        private const int MaxTopicNameLength = 256;
        public string TopicName<T>() => CreateResourceName(Guid.NewGuid(), MaxTopicNameLength);

        public string AsArn<T>()
        {
            return $"arn:aws:sns:us-east-1:000000000000:{TopicName<T>()}";
        }

        private static string CreateResourceName(Guid uuid, int maximumLength)
        {
            var name = Regex.Replace(uuid.ToString(), "[^a-zA-Z0-9_-]", string.Empty);

            return name.Length <= maximumLength ? name.ToLowerInvariant() : name.Substring(0, maximumLength);
        }
    }
}
