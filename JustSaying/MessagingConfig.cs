using System;
using System.Collections.Generic;
using System.Linq;
using JustSaying.AwsTools;

namespace JustSaying
{
    public class MessagingConfig : IMessagingConfig
    {
        public MessagingConfig()
        {
            PublishFailureReAttempts = JustSayingConstants.DEFAULT_PUBLISHER_RETRY_COUNT;
            PublishFailureBackoffMilliseconds = JustSayingConstants.DEFAULT_PUBLISHER_RETRY_INTERVAL;
            Regions = new List<string>();
        }

        public int PublishFailureReAttempts { get; set; }
        public int PublishFailureBackoffMilliseconds { get; set; }
        public IList<string> Regions { get; set; }
        public Func<string> GetActiveRegion { get; set; }

        public virtual void Validate()
        {
            if (!Regions.Any() || string.IsNullOrWhiteSpace(Regions.First()))
            {
                throw new ArgumentNullException("config.Regions", "Cannot have a blank entry for config.Regions");
            }
        }
    }
}